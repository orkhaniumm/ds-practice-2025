import sys
import os
import json
import logging
import time
import threading
import uuid
import grpc
from flask import Flask, request, jsonify
from flask_cors import CORS

# OpenTelemetry setup
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

# Tracing setup
resource = Resource(attributes={SERVICE_NAME: "orchestrator"})
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://observability:4318/v1/traces"))
)

# Metrics setup
metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint="http://observability:4318/v1/metrics"),
    export_interval_millis=5000
)
metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[metric_reader]))
meter = metrics.get_meter(__name__)

# Define metrics
order_counter = meter.create_counter("orders_total")
active_requests = meter.create_up_down_counter("active_requests")
response_hist = meter.create_histogram("response_time_ms")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure gRPC stub paths - 3 MSs by now
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
tx_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sugg_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
order_queue_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/order_queue'))
books_db_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/books_database'))

# Add directories to system path
sys.path.insert(0, fraud_path)
sys.path.insert(0, tx_path)
sys.path.insert(0, sugg_path)
sys.path.insert(0, order_queue_path)
sys.path.insert(0, books_db_path)

# Import gRPC modules for Fraud Detection, Transaction Verification, Suggestions, Order Queue, and Books Database
import fraud_detection_pb2 as fd_pb2
import fraud_detection_pb2_grpc as fd_grpc
import transaction_verification_pb2 as tv_pb2
import transaction_verification_pb2_grpc as tv_grpc
import suggestions_pb2 as sugg_pb2
import suggestions_pb2_grpc as sugg_grpc
import order_queue_pb2 as oq_pb2
import order_queue_pb2_grpc as oq_grpc
from utils.pb.books_database import books_database_pb2 as books_pb2
from utils.pb.books_database import books_database_pb2_grpc as books_pb2_grpc

# timeout for DB operations
grpc.timeout_sec = 3

app = Flask(__name__)
CORS(app)

# gRPC service addresses
FRAUD_ADDR = "fraud_detection:50051"
TX_ADDR = "transaction_verification:50052"
SUGG_ADDR = "suggestions:50053"
ORDER_QUEUE_ADDR = "order_queue:50051"
BOOKS_DB_ADDR = os.environ.get('BOOKS_DB_PRIMARY_ADDR')

# Merge multiple vector clocks into a single one
def merge_clocks(*clock_strings):
    merged = {"transaction": 0, "fraud": 0, "suggestions": 0}
    for cs in clock_strings:
        if cs:
            try:
                c = json.loads(cs)
                for k, v in c.items():
                    merged[k] = max(merged.get(k, 0), v)
            except Exception as e:
                logger.error(f"Error parsing clock: {e}")
    return json.dumps(merged)

### Threading ###
class WorkerThread(threading.Thread):
    def __init__(self, target, args=()):
        super().__init__()
        self._target = target
        self._args = args
        self.result = None

    def run(self):
        # Call the target function
        self.result = self._target(*self._args)

### Flask Routes ###
@app.route("/checkout", methods=["POST"])
def checkout():
    with tracer.start_as_current_span("orchestrator_checkout"):
        active_requests.add(1)
        start_time = time.time()

        checkout_data = request.get_json()
        if not checkout_data:
            active_requests.add(-1)
            return jsonify({"error": "Invalid request, JSON expected.", "suggestedBooks": []}), 400

        order_id = str(uuid.uuid4())
        logger.info(f"[Orchestrator] Received checkout request for order {order_id}")
        order_counter.add(1, {"status": "received"})

        init_results = {}

        def init_tx():
            with grpc.insecure_channel(TX_ADDR) as channel:
                stub = tv_grpc.VerificationServiceStub(channel)
                resp = stub.InitOrder(tv_pb2.InitOrderRequest(orderId=order_id, checkoutData=json.dumps(checkout_data)))
                init_results["tx"] = resp.clock

        def init_fd():
            with grpc.insecure_channel(FRAUD_ADDR) as channel:
                stub = fd_grpc.FraudDetectionServiceStub(channel)
                resp = stub.InitOrder(fd_pb2.FraudInitRequest(orderId=order_id, checkoutData=json.dumps(checkout_data)))
                init_results["fd"] = resp.clock

        def init_sugg():
            with grpc.insecure_channel(SUGG_ADDR) as channel:
                stub = sugg_grpc.SuggestionServiceStub(channel)
                resp = stub.InitOrder(sugg_pb2.SuggestionInitRequest(orderId=order_id, checkoutData=json.dumps(checkout_data)))
                init_results["sugg"] = resp.clock

        t1 = threading.Thread(target=init_tx)
        t2 = threading.Thread(target=init_fd)
        t3 = threading.Thread(target=init_sugg)
        t1.start(); t2.start(); t3.start()
        t1.join(); t2.join(); t3.join()

        init_clock = merge_clocks(init_results.get("tx"), init_results.get("fd"), init_results.get("sugg"))
        logger.info(f"[Orchestrator] Init clock: {init_clock}")

        tv_results = {}

        def verify_items():
            with grpc.insecure_channel(TX_ADDR) as channel:
                stub = tv_grpc.VerificationServiceStub(channel)
                resp = stub.VerifyItems(tv_pb2.VerificationRequest(orderId=order_id, clock=init_clock))
                tv_results["items"] = resp

        def verify_user():
            with grpc.insecure_channel(TX_ADDR) as channel:
                stub = tv_grpc.VerificationServiceStub(channel)
                resp = stub.VerifyUserData(tv_pb2.VerificationRequest(orderId=order_id, clock=init_clock))
                tv_results["user"] = resp

        ta = WorkerThread(target=verify_items)
        tb = WorkerThread(target=verify_user)
        ta.start(); tb.start()
        ta.join(); tb.join()

        if not tv_results["items"].success:
            active_requests.add(-1)
            response_hist.record((time.time() - start_time) * 1000)
            return jsonify({
                "orderId": order_id,
                "status": "Order Rejected",
                "message": tv_results["items"].message,
                "suggestedBooks": []
            }), 200

        if not tv_results["user"].success:
            active_requests.add(-1)
            response_hist.record((time.time() - start_time) * 1000)
            return jsonify({
                "orderId": order_id,
                "status": "Order Rejected",
                "message": tv_results["user"].message,
                "suggestedBooks": []
            }), 200

        ab_clock = merge_clocks(tv_results["items"].clock, tv_results["user"].clock)

        with grpc.insecure_channel(TX_ADDR) as channel:
            stub = tv_grpc.VerificationServiceStub(channel)
            resp_cc = stub.VerifyCreditCard(tv_pb2.VerificationRequest(orderId=order_id, clock=ab_clock))
        if not resp_cc.success:
            active_requests.add(-1)
            response_hist.record((time.time() - start_time) * 1000)
            return jsonify({
                "orderId": order_id,
                "status": "Order Rejected",
                "message": resp_cc.message,
                "suggestedBooks": []
            }), 200
        tv_clock = merge_clocks(ab_clock, resp_cc.clock)

        with grpc.insecure_channel(FRAUD_ADDR) as channel:
            stub = fd_grpc.FraudDetectionServiceStub(channel)
            resp_fd_user = stub.CheckUserDataFraud(fd_pb2.FraudRequest(orderId=order_id, clock=ab_clock))
        if resp_fd_user.isFraud:
            active_requests.add(-1)
            response_hist.record((time.time() - start_time) * 1000)
            return jsonify({
                "orderId": order_id,
                "status": "Order Rejected",
                "message": resp_fd_user.message,
                "suggestedBooks": []
            }), 200
        fd_clock_1 = merge_clocks(ab_clock, resp_fd_user.clock)

        with grpc.insecure_channel(FRAUD_ADDR) as channel:
            stub = fd_grpc.FraudDetectionServiceStub(channel)
            resp_fd_cc = stub.CheckCreditCardFraud(fd_pb2.FraudRequest(orderId=order_id, clock=fd_clock_1))
        if resp_fd_cc.isFraud:
            active_requests.add(-1)
            response_hist.record((time.time() - start_time) * 1000)
            return jsonify({
                "orderId": order_id,
                "status": "Order Rejected",
                "message": resp_fd_cc.message,
                "suggestedBooks": []
            }), 200
        fd_clock_final = merge_clocks(fd_clock_1, resp_fd_cc.clock)

        with grpc.insecure_channel(SUGG_ADDR) as channel:
            stub = sugg_grpc.SuggestionServiceStub(channel)
            book_title = checkout_data.get("items", [{}])[0].get("name", "Default Book")
            resp_sugg = stub.GenerateSuggestions(
                sugg_pb2.SuggestionRequest(orderId=order_id, clock=fd_clock_final, book_title=book_title)
            )
        sugg_clock = resp_sugg.clock
        suggestions_list = [{"title": s.title, "author": s.author} for s in resp_sugg.suggestions]

        try:
            with grpc.insecure_channel(BOOKS_DB_ADDR) as channel:
                db_stub = books_pb2_grpc.BooksDatabaseStub(channel)
                for item in checkout_data.get("items", []):
                    title = item.get("name")
                    qty   = item.get("quantity", 0)
                    stock_resp = db_stub.Read(
                        books_pb2.ReadRequest(title=title),
                        timeout=3
                    )
                    if stock_resp.stock < qty:
                        active_requests.add(-1)
                        response_hist.record((time.time() - start_time) * 1000)
                        return jsonify({
                            "orderId": order_id,
                            "status": "Order Rejected",
                            "message": f"Insufficient stock for '{title}'. Only {stock_resp.stock} left.",
                            "suggestedBooks": []
                        }), 200
        except grpc.RpcError as e:
            logger.error(f"[Orchestrator] DB error: {e}")
            active_requests.add(-1)
            response_hist.record((time.time() - start_time) * 1000)
            return jsonify({
                "orderId": order_id,
                "status": "Order Rejected",
                "message": "Book Database unavailable, please try later.",
                "suggestedBooks": []
            }), 200

        try:
            with grpc.insecure_channel(ORDER_QUEUE_ADDR) as channel:
                oq_stub = oq_grpc.OrderQueueServiceStub(channel)
                enqueue_request = oq_pb2.EnqueueRequest(
                    order=oq_pb2.Order(
                        order_id=order_id,
                        priority=1,
                        details=json.dumps(checkout_data)
                    )
                )
                eq_response = oq_stub.Enqueue(enqueue_request)
                logger.info(f"[Orchestrator] Enqueue response: {eq_response.message}")
                if not eq_response.success:
                    raise Exception(eq_response.message)
        except Exception as e:
            logger.error(f"[Orchestrator] Order enqueue failed: {e}")
            active_requests.add(-1)
            response_hist.record((time.time() - start_time) * 1000)
            return jsonify({
                "orderId": order_id,
                "status": "Order Rejected",
                "message": f"Order enqueue failed: {e}",
                "suggestedBooks": []
            }), 200

        final_clock = merge_clocks(
            init_clock,
            tv_results["items"].clock,
            tv_results["user"].clock,
            resp_cc.clock,
            resp_fd_user.clock,
            resp_fd_cc.clock,
            sugg_clock
        )
        logger.info(f"[Orchestrator] Final merged clock: {final_clock}")

        response_payload = {
            "orderId": order_id,
            "status": "Order Approved",
            "suggestedBooks": suggestions_list,
            "finalClock": json.loads(final_clock)
        }
        logger.info(f"[Orchestrator] Final decision for order {order_id}: Order Approved")
        response_hist.record((time.time() - start_time) * 1000)
        active_requests.add(-1)
        return jsonify(response_payload), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
