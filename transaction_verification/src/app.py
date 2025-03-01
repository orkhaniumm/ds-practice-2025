import sys
import os
import grpc
import json
from concurrent import futures
import logging

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure gRPC stub path
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
tx_verification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, tx_verification_grpc_path)

import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc

class VerificationService(transaction_verification_grpc.VerificationServiceServicer):
    def Verify(self, request, context):
        logger.info(f"[TransactionVerification] Received verification request for order {request.orderId}")
        
        # Parse the checkout data
        try:
            data = json.loads(request.checkoutData)
        except Exception as e:
            logger.error(f"[TransactionVerification] Failed to parse checkoutData: {e}")
            return transaction_verification.VerificationResponse(isValid=False, message="Invalid JSON")
        
        # Check the items list.
        items = data.get("items", [])
        if not items:
            logger.info("[TransactionVerification] Verification failed: items list is empty.")
            return transaction_verification.VerificationResponse(isValid=False, message="Empty items list")
        
        """
        Current logic for transaction verification is based on checking if the credit card
        number is present and is a 16-digit numeric value. More advanced verification to be
        added later.
        """
        # Check if credit card exists and has 16 digits.
        credit_card = data.get("creditCard", {})
        cc_number = credit_card.get("number", "")
        if not cc_number or len(cc_number) != 16 or not cc_number.isdigit():
            logger.info("[TransactionVerification] Verification failed: invalid credit card number.")
            return transaction_verification.VerificationResponse(isValid=False, message="Invalid credit card")
        
        logger.info(f"[TransactionVerification] Order {request.orderId} passed verification.")
        return transaction_verification.VerificationResponse(isValid=True, message="Transaction valid")

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add the VerificationService servicer to the server
    transaction_verification_grpc.add_VerificationServiceServicer_to_server(VerificationService(), server)
    # Listen on port 50052
    port = "50052"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    print(f"Server started. Listening on port {port}.")
    # Keep thread alive
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
