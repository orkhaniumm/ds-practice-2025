import sys
import os
import logging
import time
import threading
import json
import grpc
from flask import Flask, request, jsonify
from flask_cors import CORS

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure gRPC stub paths - 3 MSs by now
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
tx_verification_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
suggestions_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
# Add directories to system path
sys.path.insert(0, fraud_detection_path)
sys.path.insert(0, tx_verification_path)
sys.path.insert(0, suggestions_path)

# Import gRPC modules for Fraud Detection and Transaction Verification
import fraud_detection_pb2 as fraud_detection_pb2
import fraud_detection_pb2_grpc as fraud_detection_grpc

import transaction_verification_pb2 as tx_verification_pb2
import transaction_verification_pb2_grpc as transaction_verification_grpc

# Suggestions service to be implemented later
# import suggestions_pb2 as suggestions
# import suggestions_pb2_grpc as suggestions_grpc

# gRPC service addresses
FRAUD_DETECTION_ADDR = 'fraud_detection:50051'
TX_VERIFICATION_ADDR = 'transaction_verification:50052'
SUGGESTIONS_ADDR = 'suggestions:50053'

### Threading ###
class WorkerThread(threading.Thread):
    def __init__(self, target, args=()):
        super().__init__()
        self._target = target
        self._args = args
        self.result = None

    def run(self):
        self.result = self._target(*self._args)

### gRPC call functions ###

def perform_fraud_detection(checkout_data, order_id):
    logger.info(f"[FraudDetection] Processing order {order_id}")
    try:
        with grpc.insecure_channel(FRAUD_DETECTION_ADDR) as channel:
            stub = fraud_detection_grpc.FraudDetectionServiceStub(channel)
            request_msg = fraud_detection_pb2.FraudDetectionRequest(
                checkoutData=json.dumps(checkout_data),
                orderId=str(order_id)
            )
            response = stub.CheckFraud(request_msg)
            is_fraud = response.isFraud
            logger.info(f"[FraudDetection] Order {order_id} fraud result: {is_fraud}")
            return is_fraud
    except Exception as e:
        logger.error(f"[FraudDetection] Error for order {order_id}: {e}")
        return True  # Assume fraud in case of error.

def perform_transaction_verification(checkout_data, order_id):
    logger.info(f"[TransactionVerification] Processing order {order_id}")
    try:
        with grpc.insecure_channel(TX_VERIFICATION_ADDR) as channel:
            stub = transaction_verification_grpc.VerificationServiceStub(channel)
            request_msg = tx_verification_pb2.VerificationRequest(
                checkoutData=json.dumps(checkout_data),
                orderId=str(order_id)
            )
            response = stub.Verify(request_msg)
            is_valid = response.isValid  # Awaiting a boolean.
            logger.info(f"[TransactionVerification] Order {order_id} verification: {is_valid}")
            return is_valid
    except Exception as e:
        logger.error(f"[TransactionVerification] Error for order {order_id}: {e}")
        return False  # Treat as invalid in case of error.
    
### Flask App and Endpoints ###

app = Flask(__name__)
CORS(app)

@app.route('/', methods=['GET'])
def index():
    return "Orchestrator Service is running."

@app.route('/checkout', methods=['POST'])
def checkout():
    checkout_data = request.get_json()
    if not checkout_data:
        return jsonify({"error": "Invalid request, JSON expected."}), 400

    # Timestamp based order ID - supposed to be unique. Can further be refined
    order_id = int(time.time() * 1000)
    logger.info(f"[Orchestrator] Received checkout request for order {order_id}")

    # Threads for gRPC calls
    fraud_thread = WorkerThread(target=perform_fraud_detection, args=(checkout_data, order_id))
    tx_thread = WorkerThread(target=perform_transaction_verification, args=(checkout_data, order_id))
    
    # Threads #
    fraud_thread.start()
    tx_thread.start()

    fraud_thread.join()
    tx_thread.join()

    # Collect results
    is_fraud = fraud_thread.result
    tx_response = tx_thread.result  # This is a boolean.

    # Make final decision based on results
    if is_fraud or not tx_response:
        status = "Order Rejected"
        response_payload = {
            "orderId": order_id,
            "status": status,
            "message": "Fraud detected or transaction invalid.",
            "suggestedBooks": []  # Not used rn, but must be present
        }
    else:
        status = "Order Approved"
        response_payload = {
            "orderId": order_id,
            "status": status,
            "suggestedBooks": []
        }

    logger.info(f"[Orchestrator] Final decision for order {order_id}: {status}")
    return jsonify(response_payload)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
