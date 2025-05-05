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

        # for 2PC state
        self.temp_updates = {}

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

    # 2PC methods here
    def Prepare(self, request, context):
        self.logger.info(f"[Prepare] order={request.order_id}, '{request.title}' -> {request.new_stock}")
        self.temp_updates[request.order_id] = (request.title, request.new_stock)
        return books_pb2.PrepareResponse(ready=True, message="Prepared")

    def Commit(self, request, context):
        tup = self.temp_updates.pop(request.order_id, None)
        if tup:
            title, new_stock = tup
            self.logger.info(f"[Commit] order={request.order_id}, '{title}' -> {new_stock}")
            self._local_write(title, new_stock)
        return books_pb2.CommitResponse(success=True, message="Committed")

    def Abort(self, request, context):
        if request.order_id in self.temp_updates:
            self.temp_updates.pop(request.order_id, None)
            self.logger.info(f"[Abort] order={request.order_id}, discarded staged update")
        return books_pb2.AbortResponse(aborted=True, message="Aborted")


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
                    ack_count += 1
            except:
                pass
        return (ack_count == required)

    def Write(self, request, context):
        super().Write(request, context)
        ok = self._replicate(request)
        if not ok:
            self.logger.error("[PrimaryWrite] replication failed")
        return books_pb2.WriteResponse(success=ok)

    def Commit(self, request, context):
        resp = super().Commit(request, context)
        tup = self.store.get(request.title)
        if tup is not None:
            write_req = books_pb2.WriteRequest(title=request.title, new_stock=tup)
            self._replicate(write_req)
        return resp


# --- Server ---
def serve():
    replica_id = os.environ.get('REPLICA_ID', '')
    primary_id = os.environ.get('PRIMARY_ID', DEFAULT_PRIMARY_ID)
    port = os.environ.get('REPLICA_PORT', DEFAULT_REPLICA_PORT)
    peer_addrs = os.environ.get('PEER_ADDRS', '').split(',') if os.environ.get('PEER_ADDRS') else []

    is_primary = (replica_id == primary_id)
    servicer = PrimaryReplica(replica_id, peer_addrs) if is_primary else BaseBooksDatabaseServicer(replica_id)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    books_pb2_grpc.add_BooksDatabaseServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    role = 'PRIMARY' if is_primary else 'BACKUP'
    logging.getLogger(replica_id or __name__).info(f"[{replica_id}] {role} listening on port {port}")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()