# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: fraud_detection.proto
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
    'fraud_detection.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x15\x66raud_detection.proto\x12\x0f\x66raud_detection\"9\n\x10\x46raudInitRequest\x12\x0f\n\x07orderId\x18\x01 \x01(\t\x12\x14\n\x0c\x63heckoutData\x18\x02 \x01(\t\"3\n\x11\x46raudInitResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\x12\r\n\x05\x63lock\x18\x02 \x01(\t\".\n\x0c\x46raudRequest\x12\x0f\n\x07orderId\x18\x01 \x01(\t\x12\r\n\x05\x63lock\x18\x02 \x01(\t\"@\n\rFraudResponse\x12\x0f\n\x07isFraud\x18\x01 \x01(\x08\x12\x0f\n\x07message\x18\x02 \x01(\t\x12\r\n\x05\x63lock\x18\x03 \x01(\t2\x97\x02\n\x15\x46raudDetectionService\x12R\n\tInitOrder\x12!.fraud_detection.FraudInitRequest\x1a\".fraud_detection.FraudInitResponse\x12S\n\x12\x43heckUserDataFraud\x12\x1d.fraud_detection.FraudRequest\x1a\x1e.fraud_detection.FraudResponse\x12U\n\x14\x43heckCreditCardFraud\x12\x1d.fraud_detection.FraudRequest\x1a\x1e.fraud_detection.FraudResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'fraud_detection_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_FRAUDINITREQUEST']._serialized_start=42
  _globals['_FRAUDINITREQUEST']._serialized_end=99
  _globals['_FRAUDINITRESPONSE']._serialized_start=101
  _globals['_FRAUDINITRESPONSE']._serialized_end=152
  _globals['_FRAUDREQUEST']._serialized_start=154
  _globals['_FRAUDREQUEST']._serialized_end=200
  _globals['_FRAUDRESPONSE']._serialized_start=202
  _globals['_FRAUDRESPONSE']._serialized_end=266
  _globals['_FRAUDDETECTIONSERVICE']._serialized_start=269
  _globals['_FRAUDDETECTIONSERVICE']._serialized_end=548
# @@protoc_insertion_point(module_scope)
