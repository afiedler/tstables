import tables
import tstables
import datetime

def create_ts(self,where,name,description=None,title="",filters=None,
	expectedrows_per_partition=10000,chunkshape=None,
	byteorder=None):
	
	# The parent node of the time series
	parentnode = self._get_or_create_path(where, createparents)

	ts = tstables.TsTable(self,parentnode,description,title,filters,expectedrows_per_partition,
		chunkshape,byteorder)

	# Need to create one partition to "save" the time series. This creates a new table to persist
	# the table description
	ts.__create_partition(datetime.datetime.utcnow().date())

	return ts 	

# Augment the tables (PyTables) module's File class with the create_ts function
tables.File.create_ts = create_ts

