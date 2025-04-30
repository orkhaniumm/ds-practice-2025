import sys
import threading
import os
import grpc
import time
import logging
from concurrent import futures

GRPC_TIMEOUT_SEC = 3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# gRPC files
from utils.pb.books_database import books_database_pb2 as books_pb2
from utils.pb.books_database import books_database_pb2_grpc as books_pb2_grpc

# --- Base Servicer ---
class BaseBooksDatabaseServicer(books_pb2_grpc.BooksDatabaseServicer):
    """
    Base class containing shared logic for all replicas:
    - Basic key-value storage (in-memory dictionary)
    - Handling Read requests locally
    - Handling local Write operations (used by primary and potentially backups directly)
    """
    def __init__(self, replica_id):
        self.store = {}  # In-memory key-value store {title: stock}
        self.replica_id = replica_id
        logger.info(f"[{self.replica_id}] Initialized BaseBooksDatabaseServicer. Store: {self.store}")
        self.store["The Lord of the Rings"] = 10
        self.store["Dune"] = 5
        self.store["1984"] = 8
        logger.info(f"[{self.replica_id}] Initialized with sample data. Store: {self.store}")

    def Read(self, request, context):
        """Handles Read requests. Reads directly from the local store."""
        stock = self.store.get(request.title, 0) # Default to 0 if book not found
        logger.info(f"[{self.replica_id}] Read request for '{request.title}'. Current stock: {stock}")
        return books_pb2.ReadResponse(stock=stock)

    def _local_write(self, title, new_stock):
        """Performs the write operation on the local store."""
        self.store[title] = new_stock
        logger.info(f"[{self.replica_id}] Local write performed for '{title}'. New stock: {new_stock}")
        return True

    def Write(self, request, context):
        """
        Handles Write requests directed to this replica.
        In the Base class, this just performs a local write.
        The PrimaryReplica will override this to add replication.
        Backups might receive writes directly from the primary via this method.
        """
        logger.info(f"[{self.replica_id}] Received Write request for '{request.title}' with new_stock={request.new_stock}")
        success = self._local_write(request.title, request.new_stock)
        return books_pb2.WriteResponse(success=success)


# --- Primary Replica ---
class PrimaryReplica(BaseBooksDatabaseServicer):
    """
    Extends the base class to handle primary-specific logic:
    - Propagates writes to backup replicas.
    """
    def __init__(self, replica_id, peer_addrs):
        super().__init__(replica_id)
        self.backup_stubs = {}
        self.peer_addrs = peer_addrs
        self._connect_to_backups()
        logger.info(f"[{self.replica_id}] Initialized as PRIMARY. Connected to backups: {list(self.backup_stubs.keys())}")

    def _connect_to_backups(self):
        for peer_addr in self.peer_addrs:
            peer_id, address = peer_addr.split(':', 1)
            if peer_id != self.replica_id:
                try:
                    channel = grpc.insecure_channel(peer_addr)
                    stub = books_pb2_grpc.BooksDatabaseStub(channel)
                    self.backup_stubs[peer_id] = stub
                    logger.info(f"[{self.replica_id}] Established connection to backup: {peer_id} at {address}")
                except Exception as e:
                    logger.error(f"[{self.replica_id}] Failed to connect to backup {peer_id} at {address}: {e}")

    def _replicate_to_backup_async(self, backup_id, stub, request):
        """Helper function to send write to a single backup asynchronously."""
        try:
            logger.info(f"[{self.replica_id} - Primary] Replicating write for '{request.title}' to backup: {backup_id} (async)")
            response = stub.Write(request, timeout=GRPC_TIMEOUT_SEC)
            if not response.success:
                 logger.warning(f"[{self.replica_id} - Primary] Replication to backup {backup_id} reported failure (async).")
        except grpc.RpcError as e:
            logger.error(f"[{self.replica_id} - Primary] Failed to replicate write to backup {backup_id} (async): {e.code()} - {e.details()}")
        except Exception as e:
            logger.error(f"[{self.replica_id} - Primary] Unexpected Python error during async replication to {backup_id}: {e}", exc_info=True)

    def Write(self, request, context):
        """
        Handles Write requests:
        1. Performs the write locally.
        2. Starts background threads to propagate the write to backups asynchronously.
        3. Returns success immediately after local write.
        """
        logger.info(f"[{self.replica_id} - Primary] Received Write request for '{request.title}' with new_stock={request.new_stock}")

        # 1. Local write
        local_success = self._local_write(request.title, request.new_stock)
        if not local_success:
            logger.error(f"[{self.replica_id} - Primary] Local write failed for '{request.title}'")
            return books_pb2.WriteResponse(success=False)

        # 2. Propagate to backups asynchronously using threads
        logger.info(f"[{self.replica_id} - Primary] Initiating asynchronous replication for '{request.title}'.")
        replication_threads = []
        for backup_id, stub in self.backup_stubs.items():
            thread = threading.Thread(
                target=self._replicate_to_backup_async,
                args=(backup_id, stub, request),
                daemon=True
            )
            replication_threads.append(thread)
            thread.start()

        # 3. Return success based *only* on the local write success
        logger.info(f"[{self.replica_id} - Primary] Local write successful, returning success to client (replication proceeds in background).")
        return books_pb2.WriteResponse(success=True)
    
# --- Main Server Function ---
def serve():
    """Starts the gRPC server based on the replica's role."""
    replica_id = os.environ.get('REPLICA_ID', 'unknown_replica')
    primary_id = os.environ.get('PRIMARY_ID', 'books_database_1') # Default primary
    replica_port = os.environ.get('REPLICA_PORT', '50060')
    peer_addrs_str = os.environ.get('PEER_ADDRS', '')
    peer_addrs = [addr.strip() for addr in peer_addrs_str.split(',') if addr.strip()]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    is_primary = (replica_id == primary_id)

    if is_primary:
        logger.info(f"[{replica_id}] Starting as PRIMARY replica.")
        servicer = PrimaryReplica(replica_id, peer_addrs)
    else:
        logger.info(f"[{replica_id}] Starting as BACKUP replica.")
        servicer = BaseBooksDatabaseServicer(replica_id)

    books_pb2_grpc.add_BooksDatabaseServicer_to_server(servicer, server)

    server_address = f'[::]:{replica_port}'
    server.add_insecure_port(server_address)
    server.start()
    logger.info(f"[{replica_id}] BooksDatabase {'Primary' if is_primary else 'Backup'} Server started. Listening on {server_address}")

    try:
        while True:
            time.sleep(86400)  # Keep server running
    except KeyboardInterrupt:
        logger.info(f"[{replica_id}] Shutting down server.")
        server.stop(0)

if __name__ == '__main__':
    serve()