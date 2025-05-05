import logging
import time
import grpc
from concurrent import futures
import heapq  # Used for the priority queue
import itertools 
import threading 

import order_queue_pb2
import order_queue_pb2_grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Tie-breaking counter
counter = itertools.count()

class OrderQueueServiceServicer(order_queue_pb2_grpc.OrderQueueServiceServicer):
    def __init__(self):
        # Initialize priotity queue
        self.priority_queue = []
        heapq.heapify(self.priority_queue)
        # thread-safety 
        self.lock = threading.Lock()
        logger.info("Priority queue initialized.")

    def Enqueue(self, request, context):
        order = request.order
        count_val = next(counter)  # Get unique counter value
        #  Push the order into the priority queue with negative priority for max-heap behavior
        with self.lock:
            heapq.heappush(self.priority_queue, (-order.priority, count_val, order))
        logger.info(f"Order enqueued: {order.order_id} with priority {order.priority}")
        return order_queue_pb2.EnqueueResponse(
            success=True,
            message=f"Order {order.order_id} enqueued successfully."
        )

    def Dequeue(self, request, context):
        # Check if the priority queue is empty before popping
        with self.lock:
            if not self.priority_queue:
                # Commented out, making many logs for empty queue
                # logger.warning("Dequeue called, but the priority queue is empty.")
                return order_queue_pb2.DequeueResponse(
                    success=False,
                    message="Queue is empty."
                )
            # Pop the order with the highest priority (lowest negative value)
            neg_priority, count_val, order = heapq.heappop(self.priority_queue)

        logger.info(f"Order dequeued: {order.order_id} with priority {order.priority}")
        return order_queue_pb2.DequeueResponse(
            order=order,
            success=True,
            message=f"Order {order.order_id} dequeued successfully."
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    order_queue_pb2_grpc.add_OrderQueueServiceServicer_to_server(OrderQueueServiceServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    logger.info("Order Queue service started on port 50051.")
    try:
        while True:
            time.sleep(86400)  # Keep the server alive (1 day)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()
