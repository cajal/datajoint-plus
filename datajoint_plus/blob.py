"""
(De)serialization methods for basic datatypes and numpy.ndarrays with provisions for mutual
compatibility with Matlab-based serialization implemented by mYm.
"""

import collections
from decimal import Decimal
import datetime
import uuid
import numpy as np
from .errors import DataJointError
from datajoint.blob import (
    mxClassID,
    rev_class_id,
    dtype_list,
    type_names,
    compression,
    bypass_serialization,
    len_u64,
    len_u32,
    MatCell,
    MatStruct,
    Blob as DJBlob,
)


class Blob(DJBlob):
    def pack_blob(self, obj):
        # original mYm-based serialization from datajoint-matlab
        if isinstance(obj, MatCell):
            return self.pack_cell_array(obj)
        if isinstance(obj, MatStruct):
            return self.pack_struct(obj)
        if isinstance(obj, np.ndarray) and obj.dtype.fields is None:
            return self.pack_array(obj)

        # blob types in the expanded dj0 blob format
        self.set_dj0()
        if not isinstance(obj, (np.ndarray, np.number)):
            # python built-in data types
            if isinstance(obj, bool):
                return self.pack_bool(obj)
            if isinstance(obj, int):
                return self.pack_int(obj)
            if isinstance(obj, complex):
                return self.pack_complex(obj)
            if isinstance(obj, float):
                return self.pack_float(obj)
        if isinstance(obj, np.ndarray) and obj.dtype.fields:
            return self.pack_recarray(np.array(obj))
        if isinstance(obj, np.number):
            return self.pack_array(np.array(obj))
        if isinstance(obj, np.bool_):
            return self.pack_array(np.array(obj))
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return self.pack_datetime(obj)
        if isinstance(obj, Decimal):
            return self.pack_decimal(obj)
        if isinstance(obj, uuid.UUID):
            return self.pack_uuid(obj)
        if isinstance(obj, collections.Mapping):
            return self.pack_dict(obj)
        if isinstance(obj, str):
            return self.pack_string(obj)
        if isinstance(obj, collections.ByteString):
            return self.pack_bytes(obj)
        if isinstance(obj, collections.MutableSequence):
            return self.pack_list(obj)
        if isinstance(obj, collections.Sequence):
            return self.pack_tuple(obj)
        if isinstance(obj, collections.Set):
            return self.pack_set(obj)
        if obj is None:
            return self.pack_none()
        raise DataJointError("Packing object of type %s currently not supported!" % type(obj))


def pack(obj, compress=True):
    if bypass_serialization:
        # provide a way to move blobs quickly without de/serialization
        assert isinstance(obj, bytes) and obj.startswith((b'ZL123\0', b'mYm\0', b'dj0\0'))
        return obj
    return Blob().pack(obj, compress=compress)


def unpack(blob, squeeze=False):
    if bypass_serialization:
        # provide a way to move blobs quickly without de/serialization
        assert isinstance(blob, bytes) and blob.startswith((b'ZL123\0', b'mYm\0', b'dj0\0'))
        return blob
    if blob is not None:
        return Blob(squeeze=squeeze).unpack(blob)
