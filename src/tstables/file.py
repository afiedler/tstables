import tables
import tstables
import datetime

def create_ts(self,where,name,description=None,title="",filters=None,
	expectedrows_per_partition=10000,chunkshape=None,
	byteorder=None,createparents=False):
	
	# The parent node of the time series
	tsnode = self.create_group(where,name,title,filters,createparents)

	# Decorate with TsTables attributes
	tsnode._v_attrs._TS_TABLES_CLASS='TIMESERIES'
	tsnode._v_attrs._TS_TABLES_VERSION='0.0.1'

	ts = tstables.TsTable(self,tsnode,description,title,filters,expectedrows_per_partition,
		chunkshape,byteorder)

	# Need to create one partition to "save" the time series. This creates a new table to persist
	# the table description
	ts._TsTable__create_partition(datetime.datetime.utcnow().date())

	return ts 	



