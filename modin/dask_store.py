import os
import uuid

import dask.distributed
import pandas


class DiskStore(object):
    """A simple disk storage for Pandas objects

    TODO: Does not clean the disk!!!

    Args:
        basepath (str): Directory for storing data files
    """

    def __init__(self, basepath="/tmp/modin_store"):
        self.basepath = basepath
        if not os.path.exists(basepath):
            os.makedirs(basepath)
        assert os.path.isdir(basepath)

    def put(self, obj):
        filename = str(uuid.uuid4()) + ".pickle"
        obj.to_pickle(os.path.join(self.basepath, filename))
        return filename

    def get(self, filename):
        return pandas.read_pickle(os.path.join(self.basepath, filename))

    def __getitem__(self, filename):
        return self.get(filename)

    def put_wrap(self, obj):
        """Stores obj and returns dask.delayed object which evaluates to obj"""
        filename = self.put(obj)
        return dask.delayed(pandas.read_pickle)(os.path.join(self.basepath, filename))
