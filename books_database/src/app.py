import os
import time
import logging
import threading
import grpc
from concurrent import futures

# --- gRPC Imports ---
from utils.pb.books_database import books_database_pb2 as books_pb2
from utils.pb.books_database import books_database_pb2_grpc as books_pb2_grpc

# --- Constants ---
GRPC_TIMEOUT_SEC = 3         # Timeout for gRPC calls
DEFAULT_PRIMARY_ID = 'books_database_1'
DEFAULT_REPLICA_PORT = '50060'

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s'
)


# --- Base Service Executor ---
class BaseBooksDatabaseServicer(books_pb2_grpc.BooksDatabaseServicer):
    # Base class for all replicas: in-memory key-value storage and local handling of ops
    def __init__(self, replica_id: str):
        self.replica_id = replica_id
        self.logger = logging.getLogger(self.replica_id)
        self.store = {}
        self.store_lock = threading.Lock()

        # Seed sample data
        self.store.update({
            'The Lord of the Rings': 10,
            'Dune': 5,
            '1984': 8,
        })
        self.logger.info(f"[{self.replica_id}] Initialized store with the data: {self.store}")

    def Read(self, request, context):
        # Handles Read RPC: fetch stock for a given title from local store.
        stock = self.store.get(request.title, 0)
        self.logger.info(f"[Read] '{request.title}' -> stock={stock}")
        return books_pb2.ReadResponse(stock=stock)

    def _local_write(self, title: str, new_stock: int) -> bool:
        # Write a new stock for the book title in local store
        with self.store_lock:
            self.store[title] = new_stock
            self.logger.info(f"[LocalWrite] '{title}' := {new_stock}")
        return True

    def Write(self, request, context):
        # Write RPC - a local write "backups"
        self.logger.info(f"[Write] Received write for '{request.title}' -> new_stock={request.new_stock}")
        success = self._local_write(request.title, request.new_stock)
        return books_pb2.WriteResponse(success=success)

    def DecrementStock(self, request, context):
        # Decrement the stock if sufficient, otherwise gets failure
        title = request.title
        qty = request.quantity_to_decrement
        with self.store_lock:
            current = self.store.get(title, 0)
            if current >= qty:
                new_stock = current - qty
                self.store[title] = new_stock
                self.logger.info(f"[Decrement] '{title}' -={qty} -> {new_stock}")
                return books_pb2.DecrementStockResponse(success=True, final_stock=new_stock)
            else:
                self.logger.warning(f"[DecrementFailed] '{title}' insufficient ({current} < {qty})")
                return books_pb2.DecrementStockResponse(success=False, final_stock=current)


# --- Primary Replica Servicer ---
class PrimaryReplica(BaseBooksDatabaseServicer):
    # Primary replica: Extends BaseServicer to add synchronous replication to backups

    def __init__(self, replica_id: str, peer_addrs: list):
        super().__init__(replica_id)
        self.peer_addrs = peer_addrs                # E.g. 'id:host:port'
        self.backup_stubs = {}
        self._connect_to_backups()
        self.logger.info(f"[{self.replica_id}] Primary DB initialized. Backups: {list(self.backup_stubs)}")

    def _connect_to_backups(self):
        for addr in self.peer_addrs:
            peer_id, hostport = addr.split(':', 1)
            if peer_id == self.replica_id:
                continue
            try:
                channel = grpc.insecure_channel(addr)
                stub = books_pb2_grpc.BooksDatabaseStub(channel)
                self.backup_stubs[peer_id] = stub
                self.logger.info(f"[ConnectBackup] {peer_id}@{hostport}")
            except Exception as e:
                self.logger.error(f"[ConnectBackupFailed] {peer_id}@{hostport}: {e}")

    def _replicate(self, request: books_pb2.WriteRequest) -> bool:
        # Synchronously replicate a WriteRequest to all backups
        ack_count = 0
        required = len(self.backup_stubs)
        for pid, stub in self.backup_stubs.items():
            try:
                resp = stub.Write(request, timeout=GRPC_TIMEOUT_SEC)
                if resp.success:
                    self.logger.info(f"[ReplicateAck] {pid}")
                    ack_count += 1
                else:
                    self.logger.warning(f"[ReplicateNack] {pid}")
            except grpc.RpcError as e:
                self.logger.error(f"[ReplicateError] {pid}: {e}")
        return ack_count == required

    def Write(self, request, context):
        # Primary Write RPC - local writes first, then replicates to backups
        self.logger.info(f"[PrimaryWrite] '{request.title}' -> {request.new_stock}")
        if not self._local_write(request.title, request.new_stock):
            return books_pb2.WriteResponse(success=False)
        if not self._replicate(request):
            self.logger.error(f"[PrimaryWriteFailed] replication incomplete")
            return books_pb2.WriteResponse(success=False)
        return books_pb2.WriteResponse(success=True)

    def DecrementStock(self, request, context):
        # Primary DecrementStock RPC - local decrements, then replicates new stock
        self.logger.info(f"[PrimaryDecrement] '{request.title}' by {request.quantity_to_decrement}")
        resp = super().DecrementStock(request, context)
        if not resp.success:
            return resp
        write_req = books_pb2.WriteRequest(
            title=request.title,
            new_stock=resp.final_stock
        )
        if not self._replicate(write_req):
            self.logger.error(f"[DecrementReplicateFailed] '{request.title}'")
            return books_pb2.DecrementStockResponse(success=False, final_stock=resp.final_stock)
        return resp


# --- Server ---
def serve():
    replica_id = os.environ.get('REPLICA_ID', '')
    primary_id = os.environ.get('PRIMARY_ID', DEFAULT_PRIMARY_ID)
    port = os.environ.get('REPLICA_PORT', DEFAULT_REPLICA_PORT)
    peer_addrs = os.environ.get('PEER_ADDRS', '').split(',') if os.environ.get('PEER_ADDRS') else []

    logger = logging.getLogger(replica_id or __name__)
    is_primary = (replica_id == primary_id)
    servicer = (PrimaryReplica(replica_id, peer_addrs)
                if is_primary else BaseBooksDatabaseServicer(replica_id))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    books_pb2_grpc.add_BooksDatabaseServicer_to_server(servicer, server)
    addr = f"[::]:{port}"
    server.add_insecure_port(addr)
    server.start()
    role = 'PRIMARY' if is_primary else 'BACKUP'
    logger.info(f"[{replica_id}] {role} listening on {addr}")

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        logger.info(f"[{replica_id}] Shutting down")
        server.stop(0)


if __name__ == '__main__':
    serve()