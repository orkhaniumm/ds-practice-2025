# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: utils/pb/order_queue/order_queue.proto
# Protobuf Python Version: 5.29.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    29,
    0,
    '',
    'utils/pb/order_queue/order_queue.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n&utils/pb/order_queue/order_queue.proto\x12\norderqueue\"<\n\x05Order\x12\x10\n\x08order_id\x18\x01 \x01(\t\x12\x10\n\x08priority\x18\x02 \x01(\x05\x12\x0f\n\x07\x64\x65tails\x18\x03 \x01(\t\"2\n\x0e\x45nqueueRequest\x12 \n\x05order\x18\x01 \x01(\x0b\x32\x11.orderqueue.Order\"3\n\x0f\x45nqueueResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\x12\x0f\n\x07message\x18\x02 \x01(\t\"\x10\n\x0e\x44\x65queueRequest\"U\n\x0f\x44\x65queueResponse\x12 \n\x05order\x18\x01 \x01(\x0b\x32\x11.orderqueue.Order\x12\x0f\n\x07success\x18\x02 \x01(\x08\x12\x0f\n\x07message\x18\x03 \x01(\t2\x9b\x01\n\x11OrderQueueService\x12\x42\n\x07\x45nqueue\x12\x1a.orderqueue.EnqueueRequest\x1a\x1b.orderqueue.EnqueueResponse\x12\x42\n\x07\x44\x65queue\x12\x1a.orderqueue.DequeueRequest\x1a\x1b.orderqueue.DequeueResponseB\x0eZ\x0corderqueuepbb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'utils.pb.order_queue.order_queue_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  _globals['DESCRIPTOR']._loaded_options = None
  _globals['DESCRIPTOR']._serialized_options = b'Z\014orderqueuepb'
  _globals['_ORDER']._serialized_start=54
  _globals['_ORDER']._serialized_end=114
  _globals['_ENQUEUEREQUEST']._serialized_start=116
  _globals['_ENQUEUEREQUEST']._serialized_end=166
  _globals['_ENQUEUERESPONSE']._serialized_start=168
  _globals['_ENQUEUERESPONSE']._serialized_end=219
  _globals['_DEQUEUEREQUEST']._serialized_start=221
  _globals['_DEQUEUEREQUEST']._serialized_end=237
  _globals['_DEQUEUERESPONSE']._serialized_start=239
  _globals['_DEQUEUERESPONSE']._serialized_end=324
  _globals['_ORDERQUEUESERVICE']._serialized_start=327
  _globals['_ORDERQUEUESERVICE']._serialized_end=482
# @@protoc_insertion_point(module_scope)
