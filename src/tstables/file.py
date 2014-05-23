import tables
import tstables
import datetime
import numpy

def create_ts(self,where,name,description=None,title="",filters=None,
    expectedrows_per_partition=10000,chunkshape=None,
    byteorder=None,createparents=False):

    # Check the Description to make sure the first col is "timestamp" with type Int64
    for k in description.columns.keys():
        if description.columns[k]._v_pos == 0:
            first_col_name = k

    if first_col_name != 'timestamp':
        raise AttributeError("first column must be called 'timestamp' and have type Int64")

    if description.columns[first_col_name].dtype != numpy.dtype('int64'):
        raise AttributeError("first column must be called 'timestamp' and have type Int64")

    # The parent node of the time series
    tsnode = self.create_group(where,name,title,filters,createparents)

    try:
        # Decorate with TsTables attributes
        tsnode._v_attrs._TS_TABLES_CLASS='TIMESERIES'
        tsnode._v_attrs._TS_TABLES_VERSION='0.0.1'

        ts = tstables.TsTable(self,tsnode,description,title,filters,expectedrows_per_partition,
            chunkshape,byteorder)

        # Need to create one partition to "save" the time series. This creates a new table to persist
        # the table description
        ts._TsTable__create_partition(datetime.datetime.utcnow().date())
    except:
        # Make sure that the group is deleted if an exception is raised
        self.remove_node(tsnode,recursive=True)
        raise

    return ts



