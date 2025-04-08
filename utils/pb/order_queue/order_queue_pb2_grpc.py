# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc
import warnings

from utils.pb.order_queue import order_queue_pb2 as utils_dot_pb_dot_order__queue_dot_order__queue__pb2

GRPC_GENERATED_VERSION = '1.70.0'
GRPC_VERSION = grpc.__version__
_version_not_supported = False

try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True

if _version_not_supported:
    raise RuntimeError(
        f'The grpc package installed is at version {GRPC_VERSION},'
        + f' but the generated code in utils/pb/order_queue/order_queue_pb2_grpc.py depends on'
        + f' grpcio>={GRPC_GENERATED_VERSION}.'
        + f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}'
        + f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.'
    )


class OrderQueueServiceStub(object):
    """gRPC service definition for the order queue.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Enqueue = channel.unary_unary(
                '/orderqueue.OrderQueueService/Enqueue',
                request_serializer=utils_dot_pb_dot_order__queue_dot_order__queue__pb2.EnqueueRequest.SerializeToString,
                response_deserializer=utils_dot_pb_dot_order__queue_dot_order__queue__pb2.EnqueueResponse.FromString,
                _registered_method=True)
        self.Dequeue = channel.unary_unary(
                '/orderqueue.OrderQueueService/Dequeue',
                request_serializer=utils_dot_pb_dot_order__queue_dot_order__queue__pb2.DequeueRequest.SerializeToString,
                response_deserializer=utils_dot_pb_dot_order__queue_dot_order__queue__pb2.DequeueResponse.FromString,
                _registered_method=True)


class OrderQueueServiceServicer(object):
    """gRPC service definition for the order queue.
    """

    def Enqueue(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Dequeue(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_OrderQueueServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Enqueue': grpc.unary_unary_rpc_method_handler(
                    servicer.Enqueue,
                    request_deserializer=utils_dot_pb_dot_order__queue_dot_order__queue__pb2.EnqueueRequest.FromString,
                    response_serializer=utils_dot_pb_dot_order__queue_dot_order__queue__pb2.EnqueueResponse.SerializeToString,
            ),
            'Dequeue': grpc.unary_unary_rpc_method_handler(
                    servicer.Dequeue,
                    request_deserializer=utils_dot_pb_dot_order__queue_dot_order__queue__pb2.DequeueRequest.FromString,
                    response_serializer=utils_dot_pb_dot_order__queue_dot_order__queue__pb2.DequeueResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'orderqueue.OrderQueueService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('orderqueue.OrderQueueService', rpc_method_handlers)


 # This class is part of an EXPERIMENTAL API.
class OrderQueueService(object):
    """gRPC service definition for the order queue.
    """

    @staticmethod
    def Enqueue(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/orderqueue.OrderQueueService/Enqueue',
            utils_dot_pb_dot_order__queue_dot_order__queue__pb2.EnqueueRequest.SerializeToString,
            utils_dot_pb_dot_order__queue_dot_order__queue__pb2.EnqueueResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def Dequeue(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/orderqueue.OrderQueueService/Dequeue',
            utils_dot_pb_dot_order__queue_dot_order__queue__pb2.DequeueRequest.SerializeToString,
            utils_dot_pb_dot_order__queue_dot_order__queue__pb2.DequeueResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)
