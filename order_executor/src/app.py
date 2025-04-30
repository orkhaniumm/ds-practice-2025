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


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s'
)

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL_SEC = 7    # Interval for sending heartbeats
HEARTBEAT_TIMEOUT_SEC = 15   # Time after which a peer is considered dead
ORDER_QUEUE_POLL_INTERVAL_SEC = 1 # How often the leader polls the queue
GRPC_TIMEOUT_SEC = 3         # Timeout for gRPC calls (queue, db, ping)


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
        self.peers = [peer.strip() for peer in peer_str.split(",") if peer.strip()] if peer_str else []
        self.peer_addr_map = self._parse_peer_addrs(os.environ.get("EXECUTOR_PEER_ADDRS", ""))
        self.logger.info(f"Configured Peers: {self.peers}")
        self.logger.info(f"Peer Addresses: {self.peer_addr_map}")

        # --- State Variables ---
        self.peer_status = {peer: time.time() for peer in self.peers if peer != self.executor_id}
        self.peer_status[self.executor_id] = time.time()
        self.logger.info(f"Initial Peer Status: {self.peer_status}")

        self.is_leader = False 
        self.running = True   
        self.leader_thread = None 

        self.order_queue_stub = self._connect_to_order_queue()
        self.db_stub = self._connect_to_database()

        # Initial leader determination
        self.is_leader = self.determine_leader()
        self.logger.info(f"Initial Leader Status: {self.is_leader}")

        # Heartbeat thread
        heartbeat_thread = threading.Thread(target=self.send_heartbeats_loop, daemon=True)
        heartbeat_thread.start()
        self.logger.info("Heartbeat thread started.")

        # Leader monitoring thread
        monitor_thread = threading.Thread(target=self.monitor_leader_loop, daemon=True)
        monitor_thread.start()
        self.logger.info("Leader monitor thread started.")

        if self.is_leader:
             self._start_leader_execution_loop()

        self.logger.info("Initialization complete.")

    def _parse_peer_addrs(self, peer_addr_str):
        """Parses the EXECUTOR_PEER_ADDRS environment variable."""
        peer_addr_map = {}
        if peer_addr_str:
            for item in peer_addr_str.split(","):
                try:
                    parts = item.strip().split(':')
                    if len(parts) == 3:
                        peer_id, host, port = parts
                    elif len(parts) == 2:
                        peer_id, port = parts
                        host = peer_id
                    else: raise ValueError("Invalid format")
                    peer_addr_map[peer_id] = (host, port)
                except ValueError:
                    self.logger.error(f"Error parsing peer address '{item}'. Expected format 'peerID:host:port' or 'peerID:port'. Skipping.")
                    continue
        return peer_addr_map

    def _connect_to_order_queue(self):
        """Establishes gRPC connection to the Order Queue service."""
        oq_host = os.environ.get("ORDER_QUEUE_HOST", "order_queue")
        oq_port = os.environ.get("ORDER_QUEUE_PORT", "50051")
        oq_addr = f"{oq_host}:{oq_port}"
        try:
            self.order_queue_channel = grpc.insecure_channel(oq_addr)
            stub = order_queue_pb2_grpc.OrderQueueServiceStub(self.order_queue_channel)
            self.logger.info(f"Connected to Order Queue at {oq_addr}")
            return stub
        except Exception as e:
            self.logger.error(f"FAILED to connect to Order Queue at {oq_addr}: {e}", exc_info=True)
            return None

    def _connect_to_database(self):
        """Establishes gRPC connection to the primary Books Database replica."""
        if not books_pb2_grpc:
            self.logger.warning("BooksDatabase gRPC modules not loaded. DB functionality disabled.")
            return None
        db_addr = os.environ.get("BOOKS_DB_PRIMARY_ADDR")
        if db_addr:
            try:
                self.db_channel = grpc.insecure_channel(db_addr)
                stub = books_pb2_grpc.BooksDatabaseStub(self.db_channel)
                self.logger.info(f"Connected to Books Database Primary at {db_addr}")
                return stub
            except Exception as e:
                self.logger.error(f"FAILED to connect to Books Database at {db_addr}: {e}", exc_info=True)
                return None
        else:
            self.logger.warning("BOOKS_DB_PRIMARY_ADDR not set. DB functionality disabled.")
            return None

    # --- Leader Election & Heartbeat ---

    def determine_leader(self) -> bool:
        """
        Determines the curent leader based on peer status.
        The leader is the peer with the lexicogrphically smallest ID
        among all peers considered 'alive' (heartbeat received within timeout).
        """
        current_time = time.time()
        alive = [
            peer for peer, last_seen in self.peer_status.items()
            if (current_time - last_seen) < HEARTBEAT_TIMEOUT_SEC
        ]

        if not alive:
            return False

        alive.sort()  # Sort IDs lexicographically
        elected_leader_id = alive[0]
        is_current_leader = (self.executor_id == elected_leader_id)

        return is_current_leader

    def send_heartbeats_loop(self):
        """Periodically sends Ping requests to all peers."""
        self.logger.info("Heartbeat loop started.")
        while self.running:
            self.peer_status[self.executor_id] = time.time()

            for peer_id, (host, port) in self.peer_addr_map.items():
                if peer_id == self.executor_id:
                    continue # Don't ping self over network

                target = f"{host}:{port}"
                try:
                    with grpc.insecure_channel(target) as channel:
                        stub = order_executor_pb2_grpc.OrderExecutorServiceStub(channel)
                        response = stub.Ping(order_executor_pb2.PingRequest(), timeout=GRPC_TIMEOUT_SEC)
                        if response:
                            self.peer_status[peer_id] = time.time()
                            self.logger.info(f"[Heartbeat] Ping to {peer_id} successful.") # Log success
                except grpc.RpcError as e:
                    if e.code() in [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED]:
                        pass
                    else:
                        self.logger.error(f"[Heartbeat] Unexpected gRPC error pinging {peer_id}: {e.code()} - {e.details()}")
                except Exception as e:
                    # Log any other unexpected errors during ping
                    self.logger.error(f"[Heartbeat] Unexpected Python error pinging {peer_id}: {e}", exc_info=True)

            time.sleep(HEARTBEAT_INTERVAL_SEC)
        self.logger.info("Heartbeat loop stopped.")


    def monitor_leader_loop(self):
        """Periodically checks leader status and triggers execution loop if needed."""
        self.logger.info("Leader monitor loop started.")
        while self.running:
            current_leader_status = self.determine_leader()
            if current_leader_status != self.is_leader:
                self.logger.info(f"Leadership status changed: {self.is_leader} -> {current_leader_status}")
                self.is_leader = current_leader_status
                if self.is_leader:
                    self._start_leader_execution_loop()

            # Check leader status roughly every heartbeat interval
            time.sleep(HEARTBEAT_INTERVAL_SEC)
        self.logger.info("Leader monitor loop stopped.")

    def _start_leader_execution_loop(self):
         """Starts the leader execution loop thread if not already running."""
         if not self.leader_thread or not self.leader_thread.is_alive():
              self.logger.info("Starting leader execution loop.")
              self.leader_thread = threading.Thread(target=self.leader_execution_loop, daemon=True)
              self.leader_thread.start()


    def leader_execution_loop(self):
        """Continuously attempts to dequeue and executee orders while leader."""
        self.logger.info("Leader execution loop running.")
        while self.running and self.is_leader:
            # Call the main execution logic
            self.execute_next_order_internal()
            # Wait before polling again
            time.sleep(ORDER_QUEUE_POLL_INTERVAL_SEC)
        self.logger.info("Leader execution loop stopped (no longer leader or shutting down).")


    # --- Order Execution ---

    def execute_next_order_internal(self):
        """Internal method called by the leader loop to process one order."""
        if not self.order_queue_stub or not self.db_stub:
            self.logger.error("[Execution] Cannot execute: OQ or DB stub missing.")
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
        """ Processes a single dequeued order: chcks stock, updates if possible."""
        order_id = order.order_id
        self.logger.info(f"[Processing] Start processing order {order_id}...")

        try:
            # Parse order details
            order_data = json.loads(order.details)
            if not order_data or 'items' not in order_data or not order_data['items']:
                 self.logger.error(f"[Processing] Order {order_id}: Invalid/missing items data.")
                 return

            first_item = order_data['items'][0]
            title = first_item.get('name')
            quantity = first_item.get('quantity')

            if not title or not isinstance(quantity, int) or quantity <= 0:
                self.logger.error(f"[Processing] Order {order_id}: Invalid title or quantity in first item.")
                return

            # Interact with Database (Read Stock)
            self.logger.info(f"[DB Read] Order {order_id}: Checking stock for '{title}' (Qty: {quantity})")
            read_request = books_pb2.ReadRequest(title=title)
            read_response = self.db_stub.Read(read_request, timeout=GRPC_TIMEOUT_SEC)
            current_stock = read_response.stock
            self.logger.info(f"[DB Read] Order {order_id}: Current stock for '{title}' is {current_stock}")

            # Check Availability and Update Stock (Write)
            if current_stock >= quantity:
                new_stock = current_stock - quantity
                self.logger.info(f"[DB Write] Order {order_id}: Sufficient stock. Updating '{title}' to {new_stock}.")
                write_request = books_pb2.WriteRequest(title=title, new_stock=new_stock)
                write_response = self.db_stub.Write(write_request, timeout=GRPC_TIMEOUT_SEC)

                if write_response.success:
                    self.logger.info(f"[Execution Success] Order {order_id}: Stock for '{title}' updated to {new_stock}.")
                else:
                    self.logger.error(f"[Execution Failed] Order {order_id}: Database write failed for '{title}'.")
            else:
                # Insufficient Stock
                self.logger.warning(f"[Execution Failed] Order {order_id}: Insufficient stock for '{title}' (Req: {quantity}, Avail: {current_stock}).")

        # --- Error Handling ---
        except json.JSONDecodeError as e:
             self.logger.error(f"[Processing] Order {order_id}: Failed to parse JSON details: {e}")
        except grpc.RpcError as e:
             self.logger.error(f"[DB Interaction] Order {order_id}: gRPC error: {e.code()} - {e.details()}")
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
            self.logger.info("Is leader, attempting to execute one order now via internal method.")
            result = self.execute_next_order_internal()
            # The internal method doesn't return success/message easily here,
            # so return a generic response or inspect state if needed.
            return order_executor_pb2.ExecuteOrderResponse(success=True, message="Triggered internal execution check.")
        else:
            self.logger.info("Not leader, RPC call ignored.")
            return order_executor_pb2.ExecuteOrderResponse(success=False, message="Not the leader.")


    def Ping(self, request, context):
        """gRPC method: Responds to pings from peers for heartbeating."""
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