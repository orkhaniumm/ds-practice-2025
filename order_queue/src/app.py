import logging
import time
import grpc
from concurrent import futures

import order_queue_pb2
import order_queue_pb2_grpc

# FIFO queue
from collections import deque

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrderQueueServiceServicer(order_queue_pb2_grpc.OrderQueueServiceServicer):
    """
    Implementation of the gRPC service for managing a FIFO order queue.
    """
    def __init__(self):
        # Initialize FIFO queue.
        self.queue = deque()
        logger.info("[Initialization] OrderQueueService initialized with empty FIFO queue.")

    def Enqueue(self, request, context):
        """
        Enqueue an order into the queue.
        """
        order = request.order
        logger.info(f"[Enqueue Request] Received order with ID: {order.order_id} and priority: {order.priority}")
        
        # FIFO queue: just append the order
        self.queue.append(order)

        logger.info(f"[Enqueue Success] Order enqueued: {order.order_id} (priority={order.priority})")
        return order_queue_pb2.EnqueueResponse(
            success=True,
            message=f"Order {order.order_id} enqueued successfully."
        )

    def Dequeue(self, request, context):
        """
        Dequeue an order from the queue.
        """
        if not self.queue:
            logger.warning("[Dequeue Request] Queue is empty.")
            return order_queue_pb2.DequeueResponse(
                success=False,
                message="Queue is empty."
            )

        # FIFO queue: pop from the left
        order = self.queue.popleft()

        logger.info(f"[Dequeue Success] Order dequeued: {order.order_id}")
        return order_queue_pb2.DequeueResponse(
            order=order,
            success=True,
            message=f"Order {order.order_id} dequeued successfully."
        )

def serve():
    """
    Start the gRPC server
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    order_queue_pb2_grpc.add_OrderQueueServiceServicer_to_server(
        OrderQueueServiceServicer(), server
    )

    # Bind the server to port
    server_address = "[::]:50051"
    server.add_insecure_port(server_address)

    server.start()
    logger.info(f"[Server Start] Order Queue service started on port 50051.")

    try:
        while True:
            time.sleep(86400)  # Keep alive
    except KeyboardInterrupt:
        logger.info("[Shutdown] Shutting down Order Queue service.")
        server.stop(0)

if __name__ == "__main__":
    serve()
