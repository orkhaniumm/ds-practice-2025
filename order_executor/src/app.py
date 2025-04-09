import os
import time
import logging
import threading
import socket # For hostname resolution
from concurrent import futures
import grpc

import order_executor_pb2
import order_executor_pb2_grpc
import order_queue_pb2
import order_queue_pb2_grpc

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Heartbeat Configuration (seconds)
HEARTBEAT_INTERVAL = 7   
HEARTBEAT_TIMEOUT = 15

class OrderExecutorServiceServicer(order_executor_pb2_grpc.OrderExecutorServiceServicer):
    def __init__(self):
        # Set executor ID
        self.executor_id = os.environ.get("EXECUTOR_ID", socket.gethostname())
        
        # Retrieve peer IDs
        peer_str = os.environ.get("EXECUTOR_PEERS", "")
        if peer_str:
            self.peers = [peer.strip() for peer in peer_str.split(",") if peer.strip()]
        else:
            self.peers = []
        logger.info(f"[Initialization] Executor ID: '{self.executor_id}', Peers (from EXECUTOR_PEERS): {self.peers}")

        # Dynamic Allocation: parse peer addresses from the environment.
        self.peer_addr_map = {}
        peer_addr_str = os.environ.get("EXECUTOR_PEER_ADDRS", "")
        if peer_addr_str:
            for item in peer_addr_str.split(","):
                parts = item.strip().split(":")
                if len(parts) == 2:
                    pid, port = parts
                    host = pid
                elif len(parts) == 3:
                    pid, host, port = parts
                else:
                    logger.error(f"[Initialization] Error parsing peer address '{item}': expected 'peerID:port' or 'peerID:host:port', got {parts}")
                    continue
                self.peer_addr_map[pid] = (host, port)
        else:
            # Fallback to dynamic resolution if no addresses are provided.
            try:
                hostname, aliaslist, ipaddrlist = socket.gethostbyname_ex("tasks.order_executor")
                # First IP address is the one to be used.
                for ip in ipaddrlist:
                    self.peer_addr_map[ip] = (ip, "50052")
                    self.peers.append(ip)
            except Exception as e:
                logger.error(f"[Initialization] Dynamic peer resolution failed: {e}")
        logger.info(f"[Initialization] Peer addresses: {self.peer_addr_map}")

        # Initialize peer_status with current time for each known peer.
        self.peer_status = {}
        for peer in self.peers:
            if peer != self.executor_id:
                self.peer_status[peer] = time.time()
        self.peer_status[self.executor_id] = time.time()
        logger.info(f"[Initialization] Initial peer status: {self.peer_status}")

        # Determine if this instance is the leader.
        self.is_leader = self.determine_leader()
        logger.info(f"[Leader Election] Initially, instance '{self.executor_id}' leader status: {self.is_leader}")

        # Set up connection to Order Queue
        order_queue_host = os.environ.get("ORDER_QUEUE_HOST", "order_queue")
        order_queue_port = os.environ.get("ORDER_QUEUE_PORT", "50051")
        channel_address = f"{order_queue_host}:{order_queue_port}"
        self.order_queue_channel = grpc.insecure_channel(channel_address)
        self.order_queue_stub = order_queue_pb2_grpc.OrderQueueServiceStub(self.order_queue_channel)
        logger.info(f"[Connection] Connected to Order Queue at {channel_address}")

        # Start heartbeat sender and monitor threads
        self.running = True
        threading.Thread(target=self.send_heartbeats, daemon=True).start()
        threading.Thread(target=self.monitor_leader, daemon=True).start()

    def determine_leader(self) -> bool:
        """
        Dynamically determine if the current instance should be the leader ->
        Principles: among all alive executors (via heartbeat),
        the one with the lowest ID is elected as the leader.
        """
        current_time = time.time()
        alive = [peer for peer, last_seen in self.peer_status.items() if (current_time - last_seen) < HEARTBEAT_TIMEOUT]
        # Ensure self is included.
        if self.executor_id not in alive:
            alive.append(self.executor_id)
        alive.sort()  # Lexicographical order.
        elected = alive[0] if alive else self.executor_id
        status = (self.executor_id == elected)
        logger.info(f"[Leader Election] Alive executors: {alive}. Elected leader: {elected}. {self.executor_id} leader? {status}")
        return status

    def send_heartbeats(self):
        """
        Periodically send a heartbeat (ping RPC) to all peers.
        """
        while self.running:
            # Update self heartbeat.
            self.peer_status[self.executor_id] = time.time()
            # Iterate through each peer in the address map.
            for peer_id, (host, port) in self.peer_addr_map.items():
                # Skip self.
                if peer_id == self.executor_id:
                    continue
                target = f"{host}:{port}"
                try:
                    with grpc.insecure_channel(target) as channel:
                        stub = order_executor_pb2_grpc.OrderExecutorServiceStub(channel)
                        response = stub.Ping(order_executor_pb2.PingRequest(), timeout=2)
                        if response:
                            self.peer_status[peer_id] = time.time()
                            logger.info(f"[Heartbeat] {self.executor_id} received ping response from {peer_id} at {target}: {response.message}")
                except Exception as e:
                    logger.warning(f"[Heartbeat] {self.executor_id} failed to ping {peer_id} at {target}: {e}")
            time.sleep(HEARTBEAT_INTERVAL)

    def monitor_leader(self):
        """
        Monitor the leader status and update if necessary.
        This thread checks if the current instance is still the leader
        """
        while self.running:
            previous = self.is_leader
            self.is_leader = self.determine_leader()
            if self.is_leader != previous:
                if self.is_leader:
                    logger.info(f"[Leader Change] {self.executor_id} has become the new leader.")
                else:
                    logger.info(f"[Leader Change] {self.executor_id} is no longer the leader.")
            time.sleep(HEARTBEAT_INTERVAL)

    def ExecuteNextOrder(self, request, context):
        """
        RPC method to execute the next order in the queue.
        This method is only executed by the leader instance.
        """
        logger.info(f"[RPC] {self.executor_id} received ExecuteNextOrder request.")
        if not self.is_leader:
            msg = f"{self.executor_id} is not the leader. Skipping execution."
            logger.info(f"[Leader Check] {msg}")
            return order_executor_pb2.ExecuteOrderResponse(success=False, message=msg)
        logger.info(f"[Execution] Leader {self.executor_id} is executing the order.")
        try:
            logger.info("[Dequeue Request] Sending dequeue request to Order Queue.")
            dq_response = self.order_queue_stub.Dequeue(order_queue_pb2.DequeueRequest())
            logger.info(f"[Dequeue Response] {dq_response}")
        except Exception as e:
            error_msg = f"Error calling Dequeue RPC: {e}"
            logger.error(f"[RPC Error] {error_msg}")
            return order_executor_pb2.ExecuteOrderResponse(success=False, message=error_msg)
        if not dq_response.success:
            msg = f"No order dequeued: {dq_response.message}"
            logger.info(f"[Dequeue] {msg}")
            return order_executor_pb2.ExecuteOrderResponse(success=False, message=msg)

        order_id = dq_response.order.order_id
        log_msg = f"Order '{order_id}' executed by leader {self.executor_id}."
        logger.info(f"[Order Execution] {log_msg}")
        return order_executor_pb2.ExecuteOrderResponse(success=True, message=log_msg)

    def Ping(self, request, context):
        """
        RPC method to respond to a ping request.
        """
        reply = f"{self.executor_id} alive"
        logger.info(f"[Ping] {self.executor_id} received Ping; replying: {reply}")
        return order_executor_pb2.PingResponse(message=reply)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    order_executor_pb2_grpc.add_OrderExecutorServiceServicer_to_server(OrderExecutorServiceServicer(), server)
    port = os.environ.get("ORDER_EXECUTOR_PORT", "50052")
    server_address = f"[::]:{port}"
    server.add_insecure_port(server_address)
    server.start()
    logger.info(f"[Server Start] Order Executor service started and listening on {server_address}.")
    try:
        while True:
            time.sleep(60)  # Keep the server alive
    except KeyboardInterrupt:
        logger.info("[Shutdown] Shutting down Order Executor service.")
        server.stop(0)

if __name__ == "__main__":
    serve()
