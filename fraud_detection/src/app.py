import sys
import os
import grpc
import requests
from concurrent import futures
import logging

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure gRPC stub path
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)

import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

# Define high-risk country codes.
"""
Current fraud detectioon logic is based on rejecting requests from a predefined
list of high-risk countries that are known for fraudulent activities. Source 
IP address is extracted and sent to ipinfo.io to get the country code. Later, 
this code is compared against the list of high-risk countries to determine if
the request should be rejected. In the forthcoming sections, more prominent solutions
can be added as well.
"""
HIGH_RISK_COUNTRIES = ['NG', 'RU', 'KP', 'CN', 'IR', 'VN', 'BY']

def extract_ip(peer_info):
    try:
        # check if ip version is of v6 or v4
        if peer_info.startswith("ipv6:"):
            ip_port = peer_info[5:]
            if ip_port.startswith('['):
                ip_port = ip_port.split(']')[0].lstrip('[')
            else:
                ip_port = ip_port.split(':')[0]
        elif peer_info.startswith("ipv4:"):
            ip_port = peer_info[5:]
            ip_port = ip_port.split(':')[0]
        else:
            ip_port = peer_info
        return ip_port
    except Exception as e:  # Catch all exceptions
        logger.error(f"Error extracting IP from peer info '{peer_info}': {e}")
        return None

def get_country_from_ip(ip):
    try:    # Use ipinfo.io platform to get country code from IP address
        url = f"http://ipinfo.io/{ip}/json"
        response = requests.get(url, timeout=3)
        if response.status_code == 200:
            data = response.json()
            return data.get("country", None) # Private IP addresses will not have country code
        else:
            logger.error(f"ipinfo.io returned status {response.status_code} for IP {ip}")
    except Exception as e:
        logger.error(f"Error contacting ipinfo.io for IP {ip}: {e}")
    return None

# Define new data structures corresponding to new proto definitions.
class FraudDetectionService(fraud_detection_grpc.FraudDetectionServiceServicer):
    def CheckFraud(self, request, context):
        # Extract client IP from gRPC context.
        peer = context.peer()
        client_ip = extract_ip(peer)
        country_code = None
        if client_ip:
            country_code = get_country_from_ip(client_ip)
            if country_code:
                country_code = country_code.upper()
        
        # Perform fraud detection and make decision
        response = fraud_detection.FraudDetectionResponse()
        if country_code and country_code in HIGH_RISK_COUNTRIES:
            response.isFraud = True
            response.message = f"Access Denied for country: {country_code}"
            logger.info(f"[FraudDetection] Denied request from high-risk country: {country_code} (IP: {client_ip})")
        else:
            response.isFraud = False
            response.message = "No fraud detected"
            logger.info(f"[FraudDetection] Processed request (IP: {client_ip}, Country: {country_code or 'Unknown'})")
        return response

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add Fraud Detection service to the server
    fraud_detection_grpc.add_FraudDetectionServiceServicer_to_server(FraudDetectionService(), server)
    # Listen on port 50051
    port = "50051"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    print("Server started. Listening on port 50051.")
    # Keep thread alive
    server.wait_for_termination()

if __name__ == '__main__':
    serve()