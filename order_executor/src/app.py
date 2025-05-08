import os
import sys
import time
import logging
import threading
import socket
import json
from concurrent import futures
import grpc

# --- gRPC Imports ---
from utils.pb.order_executor import order_executor_pb2
from utils.pb.order_executor import order_executor_pb2_grpc
from utils.pb.order_queue import order_queue_pb2
from utils.pb.order_queue import order_queue_pb2_grpc
from utils.pb.books_database import books_database_pb2 as books_pb2
from utils.pb.books_database import books_database_pb2_grpc as books_pb2_grpc

# 2PC import 
from utils.pb.payment import payment_pb2, payment_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s'
)
logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL_SEC = 7    # Interval for sending heartbeats
HEARTBEAT_TIMEOUT_SEC = 15   # Time after which a peer is considered dead
ORDER_QUEUE_POLL_INTERVAL_SEC = 1 # How often the leader polls the queue
GRPC_TIMEOUT_SEC = 3         # Timeout for gRPC calls (queue, db, ping)
# 2PC payment service address
PAYMENT_ADDR = os.environ.get("PAYMENT_ADDR", "payment:50061")


class OrderExecutorServiceServicer(order_executor_pb2_grpc.OrderExecutorServiceServicer):
    """
    gRPC Servicer for the Order Executor Service.

    Handles leader election, heartbeating, and (if leader) dequeuing orders
    and interacting with the BooksDatabase to update stock.
    """
    def __init__(self):
        self.executor_id = os.environ.get("EXECUTOR_ID", socket.gethostname())
        self.logger = logging.getLogger(self.executor_id)

        self.logger.info("Initializing...")

        # --- Peer Configuration ---
        peer_str = os.environ.get("EXECUTOR_PEERS", "")
        self.peers = [p.strip() for p in peer_str.split(",") if p.strip()] if peer_str else []
        self.peer_addr_map = self._parse_peer_addrs(os.environ.get("EXECUTOR_PEER_ADDRS", ""))
        self.logger.info(f"Configured Peers: {self.peers}")
        self.logger.info(f"Peer Addresses: {self.peer_addr_map}")

        # --- State Variables ---
        now = time.time()
        self.peer_status = {peer: now for peer in self.peers if peer != self.executor_id}
        self.peer_status[self.executor_id] = now
        self.is_leader = False
        self.running = True
        self.leader_thread = None

        self.order_queue_stub = self._connect_to_order_queue()
        self.db_stub = self._connect_to_database()
        # gRPC connection to the payment service
        try:
            pay_ch = grpc.insecure_channel(PAYMENT_ADDR)
            self.payment_stub = payment_pb2_grpc.PaymentServiceStub(pay_ch)
            self.logger.info(f"Connected to PaymentService at {PAYMENT_ADDR}")
        except Exception as e:
            self.logger.error(f"FAILED to connect to PaymentService: {e}", exc_info=True)
            self.payment_stub = None

        # Initial leader determination
        self.is_leader = self.determine_leader()
        self.logger.info(f"Initial Leader Status: {self.is_leader}")

        # Heartbeat thread
        threading.Thread(target=self.send_heartbeats_loop, daemon=True).start()
        self.logger.info("Heartbeat thread started.")

        # Leader monitoring thread
        threading.Thread(target=self.monitor_leader_loop, daemon=True).start()
        self.logger.info("Leader monitor thread started.")

        if self.is_leader:
            self._start_leader_execution_loop()

        self.logger.info("Initialization complete.")

    def _parse_peer_addrs(self, peer_addr_str):
        """Parses the EXECUTOR_PEER_ADDRS environment variable."""
        peer_addr_map = {}
        for item in (peer_addr_str or "").split(","):
            item = item.strip()
            if not item:
                continue
            parts = item.split(":")
            if len(parts) == 3:
                pid, host, port = parts
            elif len(parts) == 2:
                pid, port = parts
                host = pid
            else:
                self.logger.error(f"Invalid peer addr '{item}'")
                continue
            peer_addr_map[pid] = (host, port)
        return peer_addr_map

    def _connect_to_order_queue(self):
        """Establishes gRPC connection to the Order Queue service."""
        oq_host = os.environ.get("ORDER_QUEUE_HOST", "order_queue")
        oq_port = os.environ.get("ORDER_QUEUE_PORT", "50051")
        oq_addr = f"{oq_host}:{oq_port}"
        try:
            ch = grpc.insecure_channel(oq_addr)
            stub = order_queue_pb2_grpc.OrderQueueServiceStub(ch)
            self.logger.info(f"Connected to Order Queue at {oq_addr}")
            return stub
        except Exception as e:
            self.logger.error(f"FAILED to connect to Order Queue at {oq_addr}: {e}", exc_info=True)
            return None

    def _connect_to_database(self):
        """Establishes gRPC connection to the primary Books Database replica."""
        db_addr = os.environ.get("BOOKS_DB_PRIMARY_ADDR")
        if not db_addr:
            self.logger.warning("BOOKS_DB_PRIMARY_ADDR not set. DB functionality disabled.")
            return None
        try:
            ch = grpc.insecure_channel(db_addr)
            stub = books_pb2_grpc.BooksDatabaseStub(ch)
            self.logger.info(f"Connected to Books Database Primary at {db_addr}")
            return stub
        except Exception as e:
            self.logger.error(f"FAILED to connect to Books Database at {db_addr}: {e}", exc_info=True)
            return None

    # --- Leader Election & Heartbeat ---

    def determine_leader(self) -> bool:
        """
        Determines the curent leader based on peer status.
        The leader is the peer with the lexicogrphically smallest ID
        among all peers considered 'alive' (heartbeat received within timeout).
        """
        now = time.time()
        alive = [p for p, t in self.peer_status.items() if (now - t) < HEARTBEAT_TIMEOUT_SEC]
        if not alive:
            return False
        alive.sort()
        return alive[0] == self.executor_id

    def send_heartbeats_loop(self):
        """Periodically sends Ping requests to all peers."""
        self.logger.info("Heartbeat loop started.")
        while self.running:
            self.peer_status[self.executor_id] = time.time()
            for pid, (host, port) in self.peer_addr_map.items():
                if pid == self.executor_id:
                    continue
                try:
                    addr = f"{host}:{port}"
                    ch = grpc.insecure_channel(addr)
                    stub = order_executor_pb2_grpc.OrderExecutorServiceStub(ch)
                    stub.Ping(order_executor_pb2.PingRequest(), timeout=GRPC_TIMEOUT_SEC)
                    self.peer_status[pid] = time.time()
                except:
                    pass
            time.sleep(HEARTBEAT_INTERVAL_SEC)
        self.logger.info("Heartbeat loop stopped.")


    def monitor_leader_loop(self):
        """Periodically checks leader status and triggers execution loop if needed."""
        self.logger.info("Leader monitor loop started.")
        while self.running:
            new_leader = self.determine_leader()
            if new_leader != self.is_leader:
                self.logger.info(f"Leadership status changed: {self.is_leader} -> {new_leader}")
                self.is_leader = new_leader
                if self.is_leader:
                    self._start_leader_execution_loop()

            # Check leader status roughly every heartbeat interval
            time.sleep(HEARTBEAT_INTERVAL_SEC)
        self.logger.info("Leader monitor loop stopped.")

    def _start_leader_execution_loop(self):
        """Starts the leader execution loop thread if not already running."""
        if not (self.leader_thread and self.leader_thread.is_alive()):
            self.logger.info("Starting leader execution loop.")
            self.leader_thread = threading.Thread(target=self.leader_execution_loop, daemon=True)
            self.leader_thread.start()


    def leader_execution_loop(self):
        """Continuously attempts to dequeue and execute orders while leader."""
        self.logger.info("Leader execution loop running.")
        while self.running and self.is_leader:
            # Call the main execution logic
            self.execute_next_order_internal()
            # Wait before polling again
            time.sleep(ORDER_QUEUE_POLL_INTERVAL_SEC)
        self.logger.info("Leader execution loop stopped.")

    # --- Order Execution ---

    def execute_next_order_internal(self):
        """Internal method called by the leader loop to process one order."""
        if not (self.order_queue_stub and self.db_stub and self.payment_stub):
            self.logger.error("[Execution] Cannot execute: missing stub(s).")
            return

        try:
            dq_response = self.order_queue_stub.Dequeue(
                order_queue_pb2.DequeueRequest(),
                timeout=GRPC_TIMEOUT_SEC
            )

            if dq_response.success and dq_response.order:
                # If successful dequeue, process the order
                self.logger.info(f"[Dequeue] Dequeued Order ID: {dq_response.order.order_id}")
                self._process_order(dq_response.order)

        except grpc.RpcError as e:
            self.logger.error(f"[Dequeue] RPC error: {e.code()} - {e.details()}")
        except Exception as e:
            self.logger.error(f"[Dequeue] Unexpected error: {e}", exc_info=True)

    def _process_order(self, order):
        """Processes a single dequeued order: calls DecrementStock."""
        order_id = order.order_id
        self.logger.info(f"[Processing] Start processing order {order_id}...")

        try:
            data = json.loads(order.details)
            item = data['items'][0]
            title = item.get('name')
            quantity = item.get('quantity')
            amount = quantity * 10.0  # custom price for the book

            # Check and decrement stock
            decr_resp = self.db_stub.DecrementStock(
                books_pb2.DecrementStockRequest(title=title, quantity_to_decrement=quantity),
                timeout=GRPC_TIMEOUT_SEC
            )
            if not decr_resp.success:
                self.logger.warning(f"[Execution Failed] Order {order_id}: Insufficient stock ({decr_resp.final_stock} < {quantity})")
                return

            # Use final stock as new stock for 2PC
            new_stock = decr_resp.final_stock
            self.logger.info(f"[Processing] Order {order_id}: Using new_stock={new_stock} after decrement")

            # 2PC step 1 - Prepare
            self.logger.info(f"[2PC][Prepare] order={order_id}")
            pay_p = self.payment_stub.Prepare(
                payment_pb2.PrepareRequest(order_id=order_id, amount=amount),
                timeout=GRPC_TIMEOUT_SEC
            )
            db_p = self.db_stub.Prepare(
                books_pb2.PrepareRequest(order_id=order_id, title=title, new_stock=new_stock),
                timeout=GRPC_TIMEOUT_SEC
            )

            if pay_p.ready and db_p.ready:
                # 2PC step 2 - Commit
                self.logger.info(f"[2PC][Commit] order={order_id}")
                self.payment_stub.Commit(
                    payment_pb2.CommitRequest(order_id=order_id),
                    timeout=GRPC_TIMEOUT_SEC
                )
                self.db_stub.Commit(
                    books_pb2.CommitRequest(order_id=order_id, title=title),
                    timeout=GRPC_TIMEOUT_SEC
                )
                self.logger.info(f"[Execution Success] Order {order_id} committed.")
            else:
                # 2PC Step 3 - Abort
                self.logger.warning(f"[2PC][Abort] order={order_id}")
                self.payment_stub.Abort(
                    payment_pb2.AbortRequest(order_id=order_id),
                    timeout=GRPC_TIMEOUT_SEC
                )
                self.db_stub.Abort(
                    books_pb2.AbortRequest(order_id=order_id),
                    timeout=GRPC_TIMEOUT_SEC
                )
                self.logger.warning(f"[Execution Failed] Order {order_id} aborted.")

        except json.JSONDecodeError as e:
            self.logger.error(f"[Processing] Order {order_id}: Failed to parse JSON details: {e}")
        except grpc.RpcError as e:
            self.logger.error(f"[Processing] Order {order_id}: gRPC error: {e.code()} - {e.details()}")
        except Exception as e:
            self.logger.error(f"[Processing] Order {order_id}: Unexpected error: {e}", exc_info=True)

    # --- gRPC Service Methods ---

    def ExecuteNextOrder(self, request, context):
        """
        gRPC method (potentially called externally, e.g., for testing).
        Triggers the internal execution logic *if* this instance is the leader.
        """
        self.logger.info(f"ExecuteNextOrder RPC called externally.")
        if self.is_leader:
            self.logger.info("Is leader; triggering internal execution now.")
            self.execute_next_order_internal()
            return order_executor_pb2.ExecuteOrderResponse(success=True, message="Triggered internal execution check.")
        else:
            self.logger.info("Not leader; RPC ignored.")
            return order_executor_pb2.ExecuteOrderResponse(success=False, message="Not the leader.")


    def Ping(self, request, context):
        """gRPC method: Responds to pings from peers for heartbeating."""
        self.logger.info(f"Received Ping from {context.peer()}")
        return order_executor_pb2.PingResponse(message=f"{self.executor_id} alive")

# --- Server ---
def serve():
    """Configures and runs the gRPC server."""
    executor_instance = OrderExecutorServiceServicer()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    order_executor_pb2_grpc.add_OrderExecutorServiceServicer_to_server(
        executor_instance, server
    )

    port = os.environ.get("ORDER_EXECUTOR_PORT", "50052")
    server_address = f"[::]:{port}" # Listen on all interfaces (IPv4 and IPv6)

    try:
        server.add_insecure_port(server_address)
        server.start()
        logger.info(f"[{executor_instance.executor_id}] Server started successfully. Listening on port {port}.")
        while True:
            time.sleep(86400) # Sleep for a day
    except KeyboardInterrupt:
        logger.info(f"[{executor_instance.executor_id}] Shutdown signal received.")
        executor_instance.running = False
        server.stop(0)
        logger.info(f"[{executor_instance.executor_id}] Server stopped.")
    except Exception as e:
        logger.critical(f"[{executor_instance.executor_id}] Failed to start or run server: {e}", exc_info=True)
        executor_instance.running = False
        server.stop(0)

if __name__ == "__main__":
    serve()