import os
import time
import logging
import threading
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
        self.executor_id = os.environ.get("EXECUTOR_ID", "executor_1") # get executor ID from environment variable
        peer_str = os.environ.get("EXECUTOR_PEERS", self.executor_id) # get peer ID
        self.peers = [peer.strip() for peer in peer_str.split(",")]
        logger.info(f"[Initialization] Executor ID: '{self.executor_id}', Peers: {self.peers}")

        ### Dynamic Allocation ###
        peer_addr_str = os.environ.get("EXECUTOR_PEER_ADDRS", "")
        self.peer_addr_map = {} # peer_addr_map: { peer_id: (host, port) }
        if peer_addr_str:
            for item in peer_addr_str.split(","):
                try:
                    pid, host, port = item.strip().split(":")
                    self.peer_addr_map[pid] = (host, port)
                except Exception as e:
                    logger.error(f"[Initialization] Error parsing peer address '{item}': {e}")
        logger.info(f"[Initialization] Peer addresses: {self.peer_addr_map}")

        # Initialize peer status with current time.
        self.peer_status = {peer: time.time() for peer in self.peers if peer != self.executor_id}
        self.peer_status[self.executor_id] = time.time()

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
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeats, daemon=True)
        self.heartbeat_thread.start()
        self.monitor_thread = threading.Thread(target=self.monitor_leader, daemon=True)
        self.monitor_thread.start()

    def determine_leader(self) -> bool:
        """
        Dynamically determine if the current instance should be the leader ->
        Principles: among all alive executors (via heartbeat),
        the one with the lowest ID is elected as the leader.
        """
        current_time = time.time()
        alive_peers = [peer for peer, last_seen in self.peer_status.items() if (current_time - last_seen) < HEARTBEAT_TIMEOUT]
        # Always include self if alive (which it is)
        if self.executor_id not in alive_peers:
            alive_peers.append(self.executor_id)
        alive_peers = sorted(alive_peers)
        new_leader = alive_peers[0] if alive_peers else self.executor_id
        leader_status = (self.executor_id == new_leader)
        logger.info(f"[Leader Election] Alive executors: {alive_peers}. New leader: {new_leader}. This instance: {self.executor_id} leader? {leader_status}")
        return leader_status

    def send_heartbeats(self):
        """
        Periodically send a heartbeat (ping RPC) to all peers.
        """
        while self.running:
            for peer_id, addr in self.peer_addr_map.items():
                # Skip sending heartbeat to self
                if peer_id == self.executor_id:
                    continue
                host, port = addr
                target = f"{host}:{port}"
                try:
                    with grpc.insecure_channel(target) as channel:
                        stub = order_executor_pb2_grpc.OrderExecutorServiceStub(channel)
                        response = stub.Ping(order_executor_pb2.PingRequest(), timeout=2)
                        if response:
                            # Update the last seen timestamp for the peer
                            self.peer_status[peer_id] = time.time()
                            logger.info(f"[Heartbeat] Received ping response from {peer_id} at {target}: {response.message}")
                except Exception as e:
                    logger.warning(f"[Heartbeat] Failed to ping {peer_id} at {target}: {e}")
            # Update self heartbeat
            self.peer_status[self.executor_id] = time.time()
            time.sleep(HEARTBEAT_INTERVAL)

    def monitor_leader(self):
        """
        Monitor the leader status and update if necessary.
        This thread checks if the current instance is still the leader
        """
        while self.running:
            prev_status = self.is_leader
            self.is_leader = self.determine_leader()
            if prev_status != self.is_leader:
                if self.is_leader:
                    logger.info(f"[Leader Change] Instance '{self.executor_id}' has become the new leader.")
                else:
                    logger.info(f"[Leader Change] Instance '{self.executor_id}' is no longer the leader.")
            time.sleep(HEARTBEAT_INTERVAL)  # Check the leader status periodically

    def ExecuteNextOrder(self, request, context):
        """
        RPC method to execute the next order in the queue.
        This method is only executed by the leader instance.
        """
        logger.info(f"[RPC] Received ExecuteNextOrder request on instance '{self.executor_id}'")
        
        if not self.is_leader:
            message = f"Instance '{self.executor_id}' is not the leader. Skipping order execution."
            logger.info(f"[Leader Check] {message}")
            return order_executor_pb2.ExecuteOrderResponse(success=False, message=message)

        logger.info(f"[Execution] Leader instance '{self.executor_id}' is processing the order execution.")

        # Dequeue an order from the Order Queue
        try:
            logger.info("[Dequeue Request] Sending dequeue request to Order Queue service.")
            dq_response = self.order_queue_stub.Dequeue(order_queue_pb2.DequeueRequest())
            logger.info(f"[Dequeue Response] Received response: {dq_response}")
        except Exception as e:
            error_msg = f"Error calling Order Queue Dequeue: {e}"
            logger.error(f"[RPC Error] {error_msg}")
            return order_executor_pb2.ExecuteOrderResponse(success=False, message=error_msg)

        if not dq_response.success:
            message = f"No order dequeued: {dq_response.message}"
            logger.info(f"[Dequeue] {message}")
            return order_executor_pb2.ExecuteOrderResponse(success=False, message=message)

        order_id = dq_response.order.order_id
        log_msg = f"Order '{order_id}' is being executed by leader '{self.executor_id}'."
        logger.info(f"[Order Execution] {log_msg}")

        return order_executor_pb2.ExecuteOrderResponse(success=True, message=log_msg)

    def Ping(self, request, context):
        """
        RPC method to respond to a ping request.
        """
        reply = f"{self.executor_id} alive"
        logger.info(f"[Ping] Received Ping. Responding with: {reply}")
        return order_executor_pb2.PingResponse(message=reply)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    order_executor_pb2_grpc.add_OrderExecutorServiceServicer_to_server(
        OrderExecutorServiceServicer(), server
    )
    # Bind to port
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
