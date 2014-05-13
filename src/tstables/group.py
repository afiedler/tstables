import tables
import tstables
import datetime

def timeseries_repr(self):
	"""Return a detailed string representation of the group or time series.

	Examples
	--------

	::

		>>> f = tables.open_file('data/test.h5')
		>>> f.root.group0
		/group0 (Group) 'First Group'
		  children := ['tuple1' (Table), 'group1' (Group)]

	::

		>>> f = tables.open_file('data/test_timeseries.h5')
		>>> f.root.timeseries0
		/timeseries0 (Group/Timeseries) 'A group that is also a time series'

	"""
	try:
		tstables_class = self._v_attrs._TS_TABLES_CLASS

		# Additional representation (maybe min timestamp, max timestamp) goes here
		# Don't include all of the children here!

		return "%s" % str(self)

	except AttributeError:
		rep = [
			'%r (%s)' % (childname, child.__class__.__name__)
			for (childname, child) in self._v_children.iteritems()
		]
		childlist = '[%s]' % (', '.join(rep))

		return "%s\n  children := %s" % (str(self), childlist)



def timeseries_str(self):
	"""Return a short string representation of the group or time series.

	Examples
	--------

	::

		>>> f=tables.open_file('data/test.h5')
		>>> print(f.root.group0)
		/group0 (Group) 'First Group'

	::
		>>> f = tables.open_file('data/test_timeseries.h5')
		>>> f.root.timeseries0
		/timeseries0 (Group/Timeseries) 'A group that is also a time series'

	"""

	try:
		tstables_class = self._v_attrs._TS_TABLES_CLASS
		classname = "%s/Timeseries" % self.__class__.__name__
	except AttributeError:
		classname = self.__class__.__name__

	pathname = self._v_pathname
	title = self._v_title
	return "%s (%s) %r" % (pathname, classname, title)

def get_timeseries(self):
	try:
		tstables_class = self._v_attrs._TS_TABLES_CLASS
	except AttributeError:
		return None

	ts_table = tstables.TsTable(self._v_file,self,None)

	# Need to determine the description, title, filters, expectedrows_per_partition,
	# chunkshape, byteorder
	ts_data = ts_table._TsTable__fetch_first_table()
	ts_table.table_description = ts_data.description
	ts_table.table_title = ts_data.title
	ts_table.table_filters = ts_data.filters
	ts_table.table_chunkshape = ts_data.chunkshape
	ts_table.table_byteorder = ts_data.byteorder
	ts_table.table_expectedrows_per_partition = ts_data.attrs._TS_TABLES_EXPECTEDROWS_PER_PARTITION

	return ts_table