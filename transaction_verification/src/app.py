import sys
import os
import grpc
import json
from concurrent import futures
import logging
import re
import datetime

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure gRPC stub path
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
tx_verification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, tx_verification_grpc_path)

import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc

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
    return (total % 10) == 0

# Validate expiration date: MM/YY format and not expired.
def validate_expiration_date(exp_date):
    # Check format - MM/YY
    if not re.match(r"^(0[1-9]|1[0-2])\/\d{2}$", exp_date):
        return False
    try:
        month, year = exp_date.split("/")
        month = int(month)
        year = int(year) + 2000 # Year is of 2 digits
        now = datetime.datetime.now()
        if year > now.year or (year == now.year and month >= now.month):
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"Error validating expiration date: {e}")
        return False

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
        
        # Check if credit card exists and has 16 digits.
        credit_card = data.get("creditCard", {})
        cc_number = credit_card.get("number", "")
        if not cc_number or len(cc_number) != 16 or not cc_number.isdigit():
            logger.info("[TransactionVerification] Verification failed: invalid credit card number format.")
            return transaction_verification.VerificationResponse(isValid=False, message="Invalid credit card")
        
        # Luhn algorithm check
        if not luhn_check(cc_number):
            logger.info("[TransactionVerification] Verification failed: credit card number failed Luhn check.")
            return transaction_verification.VerificationResponse(isValid=False, message="Invalid credit card (failed checksum)")
        
        # CVV check
        cvv = credit_card.get("cvv", "")
        if not cvv or not cvv.isdigit() or len(cvv) not in [3, 4]:
            logger.info("[TransactionVerification] Verification failed: invalid CVV.")
            return transaction_verification.VerificationResponse(isValid=False, message="Invalid CVV")
        
        # Expriration check
        expiration = credit_card.get("expirationDate", "")
        if not expiration or not validate_expiration_date(expiration):
            logger.info("[TransactionVerification] Verification failed: invalid or expired expiration date.")
            return transaction_verification.VerificationResponse(isValid=False, message="Invalid or expired expiration date")
        
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
