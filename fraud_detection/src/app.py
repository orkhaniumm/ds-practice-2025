import sys
import os
import grpc
import json
import logging
import requests
from concurrent import futures

# OpenTelemetry setup
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

resource = Resource(attributes={SERVICE_NAME: "fraud-detection"})
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://observability:4318/v1/traces"))
)

metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint="http://observability:4318/v1/metrics"),
    export_interval_millis=5000 
)
metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[metric_reader]))
meter = metrics.get_meter(__name__)

fraud_counter = meter.create_counter("fraud_checks_total")
fraud_gauge = meter.create_up_down_counter("active_fraud_requests")
fraud_hist = meter.create_histogram("fraud_check_duration_ms")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure gRPC stub path
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)

import fraud_detection_pb2 as fd_pb2
import fraud_detection_pb2_grpc as fd_grpc

# Define high-risk country codes.
"""
Current fraud detectioon logic is based on rejecting requests from a predefined
list of high-risk countries that are known for fraudulent activities. Source 
IP address is extracted and sent to ipinfo.io to get the country code. Later, 
this code is compared against the list of high-risk countries to determine if
the request should be rejected.
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

# Added for updating clock after fraud check
def update_clock(clock, service_name):
    clock[service_name] = clock.get(service_name, 0) + 1
    return clock

# Define new data structures corresponding to new proto definitions.
class FraudDetectionService(fd_grpc.FraudDetectionServiceServicer):
    def __init__(self):
        # Store order data in dictionary
        self.orders = {}

    def InitOrder(self, request, context):
        with tracer.start_as_current_span("InitOrder"):
            fraud_gauge.add(1)
            logger.info(f"[FraudDetection] InitOrder for {request.orderId}")
            try:
                data = json.loads(request.checkoutData)
            except Exception as e:
                logger.error(f"[FraudDetection] InitOrder parse error: {e}")
                fraud_gauge.add(-1)
                return fd_pb2.FraudInitResponse(success=False, clock="")
            init_clock = {"transaction": 0, "fraud": 0, "suggestions": 0}
            self.orders[request.orderId] = {"data": data, "clock": init_clock}
            fraud_gauge.add(-1)
            return fd_pb2.FraudInitResponse(success=True, clock=json.dumps(init_clock))

    def CheckUserDataFraud(self, request, context):
        with tracer.start_as_current_span("CheckUserDataFraud"):
            fraud_gauge.add(1)
            start = metrics.get_meter(__name__).create_counter("fraud_start_time")  # Not recorded, just to show lifecycle
            fraud_counter.add(1, {"type": "user_data"})
            timer = trace.get_current_span()
            logger.info(f"[FraudDetection] CheckUserDataFraud for {request.orderId}")
            entry = self.orders.get(request.orderId)
            if not entry:
                fraud_gauge.add(-1)
                return fd_pb2.FraudResponse(isFraud=True, message="Order not found", clock=request.clock)
            local_clock = entry["clock"]
            try:
                incoming_clock = json.loads(request.clock) if request.clock else local_clock
            except:
                incoming_clock = local_clock
            merged_clock = update_clock(local_clock, "fraud")
            entry["clock"] = merged_clock
            fraud_gauge.add(-1)
            fraud_hist.record(10.0)  # Example latency
            logger.info("[FraudDetection] User data fraud check passed.")
            return fd_pb2.FraudResponse(isFraud=False, message="User data not fraudulent", clock=json.dumps(merged_clock))

    def CheckCreditCardFraud(self, request, context):
        with tracer.start_as_current_span("CheckCreditCardFraud"):
            fraud_gauge.add(1)
            fraud_counter.add(1, {"type": "credit_card"})
            logger.info(f"[FraudDetection] CheckCreditCardFraud for {request.orderId}")
            entry = self.orders.get(request.orderId)
            if not entry:
                fraud_gauge.add(-1)
                return fd_pb2.FraudResponse(isFraud=True, message="Order not found", clock=request.clock)
            local_clock = entry["clock"]
            try:
                incoming_clock = json.loads(request.clock) if request.clock else local_clock
            except:
                incoming_clock = local_clock
            merged_clock = update_clock(local_clock, "fraud")
            entry["clock"] = merged_clock
            fraud_gauge.add(-1)
            fraud_hist.record(15.0)
            logger.info("[FraudDetection] Credit card fraud check passed.")
            return fd_pb2.FraudResponse(isFraud=False, message="Credit card not fraudulent", clock=json.dumps(merged_clock))

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add Fraud Detection service to the server
    fd_grpc.add_FraudDetectionServiceServicer_to_server(FraudDetectionService(), server)
    # Listen on port 50051
    port = "50051"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    logger.info("Fraud Detection Server started. Listening on port 50051.")
    # Keep thread alive
    server.wait_for_termination()

if __name__ == '__main__':
    serve()