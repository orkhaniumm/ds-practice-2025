import os
import logging
import grpc
from concurrent import futures

from utils.pb.payment import payment_pb2, payment_pb2_grpc

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("PaymentService")

class PaymentService(payment_pb2_grpc.PaymentServiceServicer):
    # Payment Service for the 2PC protocl
    def __init__(self): # order_id -> amount
        self.prepared = {}

    def Prepare(self, request, context):
        logger.info(f"[Prepare] order={request.order_id}, amount={request.amount}")
        self.prepared[request.order_id] = request.amount
        return payment_pb2.PrepareResponse(ready=True, message="Payment prepared")

    def Commit(self, request, context):
        if request.order_id in self.prepared:
            logger.info(f"[Commit] order={request.order_id} -> executing payment")
            del self.prepared[request.order_id]
            return payment_pb2.CommitResponse(success=True, message="Payment committed")
        else:
            logger.warning(f"[Commit] order={request.order_id} was not prepared")
            return payment_pb2.CommitResponse(success=False, message="Nothing to commit")

    def Abort(self, request, context):
        if request.order_id in self.prepared:
            logger.info(f"[Abort] order={request.order_id} -> aborting payment")
            del self.prepared[request.order_id]
        return payment_pb2.AbortResponse(aborted=True, message="Payment aborted")

# --- Server Setup ---
def serve():
    port = os.environ.get("PAYMENT_PORT", "50061")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    payment_pb2_grpc.add_PaymentServiceServicer_to_server(PaymentService(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"PaymentService listening on {port}")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
