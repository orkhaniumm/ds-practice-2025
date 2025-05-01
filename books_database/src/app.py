import sys
import threading
import os
import grpc
import time
import logging
from concurrent import futures

GRPC_TIMEOUT_SEC = 3

logging.basicConfig(level=logging.INFO)
#logger = logging.getLogger(__name__)

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
        self.store = {}
        self.replica_id = replica_id
        self.logger = logging.getLogger(self.replica_id) 
        
        self.store_lock = threading.Lock()
        
        self.logger.info(f"Initialized BaseBooksDatabaseServicer. Store: {self.store}")
        self.store["The Lord of the Rings"] = 10
        self.store["Dune"] = 5
        self.store["1984"] = 8
        self.logger.info(f"Initialized with sample data. Store: {self.store}")

    def Read(self, request, context):
        """Handles Read requests. Reads directly from the local store."""
        stock = self.store.get(request.title, 0) # Default to 0 if book not found
        self.logger.info(f"Read request for '{request.title}'. Current stock: {stock}")
        return books_pb2.ReadResponse(stock=stock)

    def _local_write(self, title, new_stock):
        """Performs the write operation on the local store atomically."""
        with self.store_lock:
           self.store[title] = new_stock
           self.logger.info(f"Local write performed for '{title}'. New stock: {new_stock}")
        return True

    def Write(self, request, context):
        """
        Handles Write requests directed to this replica.
        In the Base class, this just performs a local write.
        The PrimaryReplica will override this to add replication.
        Backups might receive writes directly from the primary via this method.
        """
        self.logger.info(f"[{self.replica_id}] Received Write request for '{request.title}' with new_stock={request.new_stock}")
        success = self._local_write(request.title, request.new_stock)
        return books_pb2.WriteResponse(success=success)
    
    def DecrementStock(self, request, context):
        """Atomically decrements stock if sufficient."""
        title = request.title
        quantity = request.quantity_to_decrement
        success = False
        final_stock = 0 
        
        with self.store_lock:
            current_stock = self.store.get(title, 0)
            if current_stock >= quantity:
                final_stock = current_stock - quantity
                self.store[title] = final_stock
                success = True
                self.logger.info(f"Decremented stock for '{title}' by {quantity}. New stock: {final_stock}")
            else:
                # Insufficient stock
                final_stock = current_stock
                success = False
                self.logger.warning(f"Insufficient stock to decrement '{title}' by {quantity}. Current: {current_stock}")
                
        return books_pb2.DecrementStockResponse(success=success, final_stock=final_stock)


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
        self.logger.info(f"Initialized as PRIMARY. Connected to backups: {list(self.backup_stubs.keys())}")

    def _connect_to_backups(self):
        for peer_addr in self.peer_addrs:
            peer_id, address = peer_addr.split(':', 1)
            if peer_id != self.replica_id:
                try:
                    channel = grpc.insecure_channel(peer_addr)
                    stub = books_pb2_grpc.BooksDatabaseStub(channel)
                    self.backup_stubs[peer_id] = stub
                    self.logger.info(f"Established connection to backup: {peer_id} at {address}")
                except Exception as e:
                    self.logger.error(f"Failed to connect to backup {peer_id} at {address}: {e}")

    def _replicate_to_backup_sync(self, backup_id, stub, request:  books_pb2.WriteRequest) -> bool:
        """Helper function to send write to a single backup synchronously."""
        try:
            self.logger.info(f"Replicating write for '{request.title}' to backup: {backup_id}")
            # Make a blocking gRPC call with timeout
            response = stub.Write(request, timeout=GRPC_TIMEOUT_SEC) # GRPC_TIMEOUT_SEC might need adjustment
            
            if response.success:
                self.logger.info(f"Replication to backup {backup_id} successful.")
                return True
            else:
                self.logger.warning(f"Replication to backup {backup_id} reported failure.")
                return False
            
        except grpc.RpcError as e:
            self.logger.error(f"Failed to replicate write to backup {backup_id}: {e.code()} - {e.details()}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected Python error during replication to {backup_id}: {e}", exc_info=True)
            return False

    def Write(self, request, context):
        """Handles Write requests on Primary: local write then SYNCHRONOUS replication."""
        self.logger.info(f"[Write] Received Write request for '{request.title}' with new_stock={request.new_stock}")

        local_success = self._local_write(request.title, request.new_stock)
        
        if not local_success: 
            self.logger.error("[Write] Local write failed for '{request.title}'")
            # If local write fails, no need to replicate
            return books_pb2.WriteResponse(success=False) 

        self.logger.info(f"[Write] Initiating replication for '{request.title}'.")
        
        replication_success_count = 0
        required_acks = len(self.backup_stubs) # Require acknowledges from ALL backups

        for backup_id, stub in self.backup_stubs.items():
            if self._replicate_to_backup_sync(backup_id, stub, request):
                replication_success_count += 1
            else:
                self.logger.warning(f"[Write] Failed to replicate to {backup_id}. Operation might fail.")
                # To enforce strict consistency where all backups must succeed:
                # return books_pb2.WriteResponse(success=False) # Fail early
                
        #    Here, we require all backups to acknowledge for simplicity.
        #    A more robust system might use a quorum (e.g., majority).
        all_replicated = (replication_success_count == required_acks)
        
        if all_replicated:
            self.logger.info(f"[Write] Synchronous replication to all {required_acks} backups successful for '{request.title}'.")
        else:
            self.logger.error(f"[Write] Synchronous replication failed for '{request.title}'. Succeeded: {replication_success_count}/{required_acks}.")

        # Return success ONLY if local write AND replication succeeded
        final_success = local_success and all_replicated
        self.logger.info(f"[Write] Overall success for '{request.title}': {final_success}. Returning response to client.")
        return books_pb2.WriteResponse(success=final_success)

    def DecrementStock(self, request, context):
        """Handles DecrementStock on Primary: local atomic op then SYNCHRONOUS replicate."""
        self.logger.info(f"[DecrementStock] Received request for '{request.title}' by {request.quantity_to_decrement}")
        
        local_response = super().DecrementStock(request, context) 

        final_success = local_response.success # Start with local success status
        
        if local_response.success:
            self.logger.info(f"[DecrementStock] Local decrement successful for '{request.title}'. Replicating new stock: {local_response.final_stock}")
            
            replication_request = books_pb2.WriteRequest(
                title=request.title,
                new_stock=local_response.final_stock 
            )
            
            replication_success_count = 0
            required_acks = len(self.backup_stubs)

            for backup_id, stub in self.backup_stubs.items():
                if self._replicate_to_backup_sync(backup_id, stub, replication_request):
                    replication_success_count += 1
                else:
                    self.logger.warning(f"[DecrementStock] Failed to replicate to {backup_id}. Operation might fail.")
                    # To enforce strict consistency where all backups must succeed:
                    # final_success = False 
                    # break # Stop trying to replicate if one fails
            
            all_replicated = (replication_success_count == required_acks)
            
            if all_replicated:
                self.logger.info(f"[DecrementStock] Synchronous replication to all {required_acks} backups successful for '{request.title}'.")
            else:
                self.logger.error(f"[DecrementStock] Synchronous replication failed for '{request.title}'. Succeeded: {replication_success_count}/{required_acks}.")
                final_success = False # Mark operation as failed if not all replicated

        else:
            # Local decrement failed
            self.logger.warning(f"[DecrementStock] Local decrement failed for '{request.title}'. No replication needed.")
            # final_success is already False from local_response.success

        self.logger.info(f"[DecrementStock] Overall success for '{request.title}': {final_success}. Returning response to client.")
        return books_pb2.DecrementStockResponse(
            success=final_success, 
            final_stock=local_response.final_stock # Return the stock state from local op
        )

# --- Main Server Function ---
def serve():
    """Starts the gRPC server based on the replica's role."""
    replica_id = os.environ.get('REPLICA_ID', 'unknown_replica')
    logger = logging.getLogger(replica_id if replica_id != 'unknown_replica' else __name__) 
    primary_id = os.environ.get('PRIMARY_ID', 'books_database_1') # Default primary
    replica_port = os.environ.get('REPLICA_PORT', '50060')
    peer_addrs_str = os.environ.get('PEER_ADDRS', '')
    peer_addrs = [addr.strip() for addr in peer_addrs_str.split(',') if addr.strip()]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    is_primary = (replica_id == primary_id)

    if is_primary:
        logger.info(f"Starting as PRIMARY replica.")
        servicer = PrimaryReplica(replica_id, peer_addrs)
    else:
        logger.info(f"Starting as BACKUP replica.")
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
        logger.info(f"Shutting down server.")
        server.stop(0)

if __name__ == '__main__':
    serve()