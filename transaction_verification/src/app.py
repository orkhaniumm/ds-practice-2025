import sys
import os
import json
import logging
import re
import datetime
from concurrent import futures
import grpc

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure gRPC stub path
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
tx_verification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, tx_verification_grpc_path)

import transaction_verification_pb2 as tv_pb2
import transaction_verification_pb2_grpc as tv_grpc

# Luhn algorithm for credit card number validation - https://www.geeksforgeeks.org/luhn-algorithm/
def luhn_check(card_number):
    total = 0
    num_digits = len(card_number)
    oddeven = num_digits & 1
    for count in range(num_digits):
        digit = int(card_number[count])
        if not ((count & 1) ^ oddeven):
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    result = (total % 10) == 0
    # Log the result of the Luhn check
    logger.info(f"[Luhn Check] Card number {card_number}: Check result = {result}")
    return result

# Validate expiration date: MM/YY format and not expired.
def validate_expiration_date(exp_date):
    if not re.match(r"^(0[1-9]|1[0-2])\/\d{2}$", exp_date):
        # Log the expiration date format error
        logger.info(f"[Expiration Date] Expiration date {exp_date} does not match MM/YY format")
        return False
    try:
        month, year = exp_date.split("/")
        month = int(month)
        year = int(year) + 2000 # Year is of 2 digits
        now = datetime.datetime.now()
        valid = (year > now.year) or (year == now.year and month >= now.month)
        logger.info(f"[Expiration Date] Expiration date {exp_date}: Valid = {valid}")
        return valid
    except Exception as e:
        logger.error(f"[Expiration Date] Error validating expiration date {exp_date}: {e}")
        return False

# Merge two vector clocks and increment the transaction index.
def merge_and_increment(local_clock, incoming_clock, idx_name="transaction"):
    merged = {}
    for key in set(local_clock.keys()).union(incoming_clock.keys()):
        merged[key] = max(local_clock.get(key, 0), incoming_clock.get(key, 0))
    merged[idx_name] = merged.get(idx_name, 0) + 1
    logger.info(f"[Vector Clock] Merged clock updated with '{idx_name}': {merged}")
    return merged

# Define the gRPC service
class VerificationService(tv_grpc.VerificationServiceServicer):
    def __init__(self):
        # Each order is stored with its checkout data and vector clock.
        self.orders = {} 

    def InitOrder(self, request, context):
        logger.info(f"[TransactionVerification] InitOrder for {request.orderId}")
        # Parse the checkout data
        try:
            data = json.loads(request.checkoutData)
            logger.info(f"[TransactionVerification] Parsed checkout data for order {request.orderId}: {data}")
        except Exception as e:
            logger.error(f"[TransactionVerification] InitOrder parse error for order {request.orderId}: {e}")
            return tv_pb2.InitOrderResponse(success=False, clock="")
        # Initialize the vector clock
        init_clock = {"transaction": 0, "fraud": 0, "suggestions": 0}
        self.orders[request.orderId] = {"data": data, "clock": init_clock}
        logger.info(f"[TransactionVerification] Initialized vector clock for order {request.orderId}: {init_clock}")
        return tv_pb2.InitOrderResponse(success=True, clock=json.dumps(init_clock))

    def VerifyItems(self, request, context):
        logger.info(f"[TransactionVerification] VerifyItems for {request.orderId}")
        entry = self.orders.get(request.orderId)
        if not entry:
            logger.error(f"[TransactionVerification] Order {request.orderId} not found during VerifyItems")
            return tv_pb2.VerificationResponse(success=False, message="Order not found", clock=request.clock)
        local_clock = entry["clock"]
        try:
            incoming_clock = json.loads(request.clock) if request.clock else local_clock
        except Exception as e:
            logger.error(f"[TransactionVerification] Error parsing clock in VerifyItems: {e}")
            incoming_clock = local_clock
        # Merge the clocks
        merged_clock = merge_and_increment(local_clock, incoming_clock, "transaction")
        entry["clock"] = merged_clock
        items = entry["data"].get("items", [])
        if not items:
            logger.error(f"[TransactionVerification] Order {request.orderId} failed items check: empty items list")
            return tv_pb2.VerificationResponse(success=False, message="Empty items list", clock=json.dumps(merged_clock))
        logger.info(f"[TransactionVerification] Order {request.orderId} items check passed")
        return tv_pb2.VerificationResponse(success=True, message="Items ok", clock=json.dumps(merged_clock))

    def VerifyUserData(self, request, context):
        logger.info(f"[TransactionVerification] VerifyUserData for {request.orderId}")
        entry = self.orders.get(request.orderId)
        if not entry:
            logger.error(f"[TransactionVerification] Order {request.orderId} not found during VerifyUserData")
            return tv_pb2.VerificationResponse(success=False, message="Order not found", clock=request.clock)
        local_clock = entry["clock"]
        try:
            incoming_clock = json.loads(request.clock) if request.clock else local_clock
        except Exception as e:
            logger.error(f"[TransactionVerification] Error parsing clock in VerifyUserData: {e}")
            incoming_clock = local_clock
        merged_clock = merge_and_increment(local_clock, incoming_clock, "transaction")
        entry["clock"] = merged_clock
        user = entry["data"].get("user", {})
        # Check the mandatory fields
        if not user.get("name") or not user.get("contact"):
            logger.error(f"[TransactionVerification] Order {request.orderId} failed user data check: incomplete data")
            return tv_pb2.VerificationResponse(success=False, message="User data incomplete", clock=json.dumps(merged_clock))
        logger.info(f"[TransactionVerification] Order {request.orderId} user data check passed")
        return tv_pb2.VerificationResponse(success=True, message="User data ok", clock=json.dumps(merged_clock))

    def VerifyCreditCard(self, request, context):
        logger.info(f"[TransactionVerification] VerifyCreditCard for {request.orderId}")
        entry = self.orders.get(request.orderId)
        if not entry:
            logger.error(f"[TransactionVerification] Order {request.orderId} not found during VerifyCreditCard")
            return tv_pb2.VerificationResponse(success=False, message="Order not found", clock=request.clock)
        local_clock = entry["clock"]
        try:
            incoming_clock = json.loads(request.clock) if request.clock else local_clock
        except Exception as e:
            logger.error(f"[TransactionVerification] Error parsing clock in VerifyCreditCard: {e}")
            incoming_clock = local_clock
        merged_clock = merge_and_increment(local_clock, incoming_clock, "transaction")
        entry["clock"] = merged_clock

        # Retrieve card details
        credit_card = entry["data"].get("creditCard", {})
        cc_number = credit_card.get("number", "")
        expiration = credit_card.get("expirationDate", "")
        cvv = credit_card.get("cvv", "")
        
        # Check card number format
        if not cc_number or len(cc_number) != 16 or not cc_number.isdigit():
            logger.error(f"[TransactionVerification] Order {request.orderId} failed credit card format check")
            return tv_pb2.VerificationResponse(success=False, message="Invalid credit card format", clock=json.dumps(merged_clock))
        # Luhn
        if not luhn_check(cc_number):
            logger.error(f"[TransactionVerification] Order {request.orderId} failed Luhn check on credit card")
            return tv_pb2.VerificationResponse(success=False, message="Invalid credit card (failed checksum)", clock=json.dumps(merged_clock))
        # Expiration date 
        if not expiration or not validate_expiration_date(expiration):
            logger.error(f"[TransactionVerification] Order {request.orderId} failed expiration date check: {expiration} is invalid or expired")
            return tv_pb2.VerificationResponse(success=False, message="Invalid or expired expiration date", clock=json.dumps(merged_clock))
        # CVV 
        if not cvv or not cvv.isdigit() or len(cvv) not in [3, 4]:
            logger.error(f"[TransactionVerification] Order {request.orderId} failed CVV check: CVV provided is '{cvv}'")
            return tv_pb2.VerificationResponse(success=False, message="Invalid CVV", clock=json.dumps(merged_clock))
        
        logger.info(f"[TransactionVerification] Order {request.orderId} credit card check passed")
        return tv_pb2.VerificationResponse(success=True, message="Credit card ok", clock=json.dumps(merged_clock))

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add the VerificationService servicer to the server
    tv_grpc.add_VerificationServiceServicer_to_server(VerificationService(), server)
    # Listen on port 50052
    port = "50052"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    logger.info("Transaction Verification Server started. Listening on port 50052.")
    # Keep thread alive
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
