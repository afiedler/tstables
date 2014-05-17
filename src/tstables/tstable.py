import pytz
import datetime
import tables
import numpy
import numpy.lib.recfunctions
import pandas
import re

class TsTable:
	EPOCH = datetime.datetime(1970,1,1,tzinfo=pytz.utc)

	def __init__(self,pt_file,root_group,description,title="",filters=None,
		expectedrows_per_partition=10000,chunkshape=None,byteorder=None):
		self.file = pt_file
		self.root_group = root_group
		self.table_description = description
		self.table_title = title
		self.table_filters = filters
		self.table_expectedrows = expectedrows_per_partition
		self.table_chunkshape = chunkshape
		self.table_byteorder = byteorder

	@staticmethod	
	def __dtrange_to_partition_ranges(start_dt,end_dt):

		# We assume that start_ts and end_ts are in UTC at this point

		# Partition ranges are tuples with a partition_start datetime and partition_end datetime
		# The range is inclusive of the start and excludes the end: [partition_start, partition_end)
		delta = end_dt - start_dt
		num_days = delta.days + (1 if (delta.seconds > 0) | (delta.microseconds > 0) else 0)
		partition_subsets = dict.fromkeys([
			start_dt.date() + datetime.timedelta(days=x) for x in range(0,num_days)])

		for k in partition_subsets.keys():
			if start_dt.date() == k:
				partition_start = start_dt
			else:
				partition_start = datetime.datetime.combine(k,pytz.utc.localize(datetime.time.min))
			
			if end_dt.date() == k:
				partition_end = end_dt
			else:
				partition_end = datetime.datetime.combine(
					k+datetime.timedelta(days=1),pytz.utc.localize(datetime.time.min))
			
			partition_subsets[k] = partition_start, partition_end

		return partition_subsets
	
	@classmethod
	def __dt_to_ts(self,dt):
		delta = dt - self.EPOCH
		ts = numpy.int64(delta.total_seconds()) # This will strip off fractional seconds
		ts = ts * 1000 # shift to milliseconds
		ts = ts + numpy.int64(delta.microseconds/1000)
		return ts

	@classmethod
	def __ts_to_dt(self,ts):
		# Trying to avoid a lossy conversion here. If we were to cast the ts as a timedelta using
		# just milliseconds, we might overflow the buffer
		ts_milliseconds = ts % 1000
		ts_seconds = (ts - (ts % 1000)) % 86400
		ts_days = numpy.int64(numpy.divide(ts - ts_milliseconds - ts_seconds*1000,86400000))
		# The reason we do it this way it to attempt to avoid overflows for any component
		dt = self.EPOCH + datetime.timedelta(days=int(ts_days),
			seconds=int(ts_seconds),microseconds=int(ts_milliseconds)*1000)
		return dt

	def __v_dtype(self):
		return tables.description.dtype_from_descr(self.table_description)

	def __fetch_rows_from_partition(self,partition_date,start_dt,end_dt):
		try:
			y_group = self.root_group._v_groups[partition_date.strftime('y%Y')]
			m_group = y_group._v_groups[partition_date.strftime('m%m')]
			d_group = m_group._v_groups[partition_date.strftime('d%d')]
		except KeyError:
			# If the partition group is missing, then return an empty array
			return numpy.ndarray(shape=0,dtype=self.__v_dtype())

		return d_group.ts_data.read_where('(timestamp >= {0}) & (timestamp < {1})'.format(
			self.__dt_to_ts(start_dt),self.__dt_to_ts(end_dt)))

	def __fetch_first_table(self):
		y_group = self.root_group._f_list_nodes()[0]
		m_group = y_group._f_list_nodes()[0]
		d_group = m_group._f_list_nodes()[0]
		return d_group.ts_data

	def __fetch_last_table(self):
		y_group = sorted(self.root_group._f_list_nodes(), key=lambda x: x._v_name)[-1]
		m_group = sorted(y_group._f_list_nodes(), key=lambda x: x._v_name)[-1]
		d_group = sorted(m_group._f_list_nodes(), key=lambda x: x._v_name)[-1]
		return d_group.ts_data

	def __get_max_ts(self):
		max_group_dt = None
		max_ts = None
		for group in self.root_group._f_walk_groups():
			m = re.search('y([0-9]{4})/m([0-9]{2})/d([0-9]{2})',group._v_pathname)
			if m is not None:
				group_dt = datetime.date(int(m.group(1)),int(m.group(2)),int(m.group(3)))
			else:
				continue

			if (max_group_dt is not None) and (max_group_dt < group_dt):
				
				if group.ts_data.nrows == 0:
					group_max_ts = None
				else:
					group_max_ts = group.ts_data.cols.timestamp[-1]

				if (group_max_ts is not None) and (max_ts is None or max_ts < group_max_ts):
					max_ts = group_max_ts
					max_group_dt = group_dt
			elif (max_group_dt is None):
			
				if group.ts_data.nrows == 0:
					group_max_ts = None
				else:
					group_max_ts = group.ts_data.cols.timestamp[-1]

				if (group_max_ts is not None):
					max_ts = group_max_ts
					max_group_dt = group_dt

		
		return max_ts

	def __get_min_ts(self):
		min_group_dt = None
		min_ts = None
		for group in self.root_group._f_walk_groups():
			m = re.search('y([0-9]{4})/m([0-9]{2})/d([0-9]{2})',group._v_pathname)
			if m is not None:
				group_dt = datetime.date(int(m.group(1)),int(m.group(2)),int(m.group(3)))
			else:
				continue

			if (min_group_dt is not None) and (min_group_dt > group_dt):
				if group.ts_data.nrows == 0:
					group_min_ts = None
				else:
					group_min_ts = group.ts_data.cols.timestamp[0]

				if (group_min_ts is not None) and (min_ts is None or min_ts > group_min_ts):
					min_ts = group_min_ts
					min_group_dt = group_dt
			elif (min_group_dt is None):
				
				if group.ts_data.nrows == 0:
					group_min_ts = None
				else:
					group_min_ts = group.ts_data.cols.timestamp[0]

				if (group_min_ts is not None):
					min_ts = group_min_ts
					min_group_dt = group_dt

		
		return min_ts



	def read_range(self,start_dt,end_dt,as_pandas_dataframe=True):
		# Convert start_dt and end_dt to UTC if they are naive
		if start_dt.tzinfo is None:
			start_dt = pytz.utc.localize(start_dt)
		if end_dt.tzinfo is None:
			end_dt = pytz.utc.localize(end_dt)
		

		partitions = self.__dtrange_to_partition_ranges(start_dt,end_dt)
		sorted_pkeys = sorted(partitions.keys())

		# Start with an empty array
		result = numpy.ndarray(shape=0,dtype=self.__v_dtype())

		for p in sorted_pkeys:
			result = numpy.concatenate(
				(result,self.__fetch_rows_from_partition(p,start_dt,end_dt)))

		# Turn into a pandas DataFrame with a timeseries index
		if as_pandas_dataframe:
			result = pandas.DataFrame.from_records(result,
				index=result['timestamp'].astype('datetime64[ms]'),
				exclude=['timestamp'])

		return result


	def append(self,rows):
		# This part is specific to pandas support. If rows is a pandas DataFrame, convert it to a
		# format suitable to PyTables
		if rows.__class__ == pandas.core.frame.DataFrame:
			if rows.index.__class__ != pandas.tseries.index.DatetimeIndex:
				raise ValueError('when rows is a DataFrame, the index must be a DatetimeIndex.')

			# Convert the datetime64 index to milliseconds and then to int64
			indexint64 = rows.index.values.astype('datetime64[ms]').astype('int64')

			# Convert to records, excluding the index
			records = rows.to_records(index=False)

			# Merge timestamp column
			rows = numpy.lib.recfunctions.merge_arrays((indexint64,records))

		# Try to convert the object into a recarray compliant with table. This code is stolen from
		# PyTable's append method.
		try:
			iflavor = tables.flavor.flavor_of(rows)
			if iflavor != 'python':
				rows = tables.flavor.array_as_internal(rows,iflavor)

			wbufRA = numpy.rec.array(rows, dtype=self.__fetch_first_table().description._v_dtype)
		except Exception as exc:
			raise ValueError("rows parameter cannot be converted into a recarray object compliant "
							 "with table '%s'.  The error was: <%s>" % (str(self), exc))

		# Confirm that first column is Int64. This is an additional constraint of TsTables.		
		if not wbufRA.dtype[0] == numpy.dtype('int64'):
			raise ValueError("first column must be of type numpy.int64.")

		# We also need to confirm that the rows are sorted by timestamp. This is an additional
		# constraint of TsTables.
		prev_ts = numpy.iinfo('int64').min
		for r in wbufRA: # probably not ideal to loop here. is there a faster way?
			if r[0] < prev_ts:
				raise ValueError("timestamp column must be sorted in ascending order.")

		# Array is sorted at this point, so min and max are easy to get
		min_ts = wbufRA[0][0]
		max_ts = wbufRA[-1][0]

		# Confirm that min is >= to the TsTable's max_ts
		if min_ts < (self.__get_max_ts() or numpy.iinfo('int64').min):
			raise ValueError("rows start prior to the end of existing rows, so they cannot be "
							 "appended.")

		# wbufRA is ready to be inserted at this point. Chop it up into partitions.
		min_dt = self.__ts_to_dt(min_ts)
		max_dt = self.__ts_to_dt(max_ts)
		possible_partitions = self.__dtrange_to_partition_ranges(min_dt,max_dt)

		sorted_pkeys = sorted(possible_partitions.keys())
		split_on_ts = []

		# For each partition, we are splitting on the end date
		for p in sorted_pkeys:
			split_on_ts.append(self.__dt_to_ts(possible_partitions[p][1]))

		# Drop the last end date, since there is no need to split where nothing follows
		split_on_ts.pop()

		# Now, we need to loop through the entire array to be imported and figure out which indexes
		# to split on. Ideally, this loop could be combined with the loop above that checks for
		# sorting.
		split_on_idx = []
		cursor = 0
		for ts_split in split_on_ts:
			while (cursor < wbufRA.size) and (wbufRA['timestamp'][cursor] < ts_split):
				cursor = cursor + 1
			
			split_on_idx.append(cursor)

		# Need to potentially backfill with the last timestamp if split_on_idx is not the same
		# length as split_on_ts
		if len(split_on_idx) < len(split_on_ts):
			while len(split_on_idx) < len(split_on_ts):
				split_on_idx.append(wbufRA[-1][0])

		# Now, split the array
		split_wbufRA = numpy.split(wbufRA,split_on_idx)

		# Save each partition
		for idx,p in enumerate(sorted_pkeys):
			self.__append_rows_to_partition(p,split_wbufRA[idx])

	@staticmethod
	def __partition_date_to_path_array(partition_dt):
		"""Converts a partition date to an array of partition names
		"""

		return [partition_dt.strftime('y%Y'),partition_dt.strftime('m%m'),partition_dt.strftime('d%d')]

	def __append_rows_to_partition(self,partition_dt,rows):
		"""Appends rows to a partition (which might not exist yet, and will then be created)

		The rows argument is assumed to be sorted and *only* contain rows that have timestamps that
		are valid for this partition.
		"""

		ts_data = self.__fetch_or_create_partition_table(partition_dt)
		ts_data.append(rows)
	
	def __fetch_partition_group(self,partition_dt):
		"""Fetches a partition group, or returns `False` if the partition group does not exist
		"""

		try:
			p_array = self.__partition_date_to_path_array(partition_dt)
			return self.root_group._f_get_child(p_array[0])._f_get_child(p_array[1])._f_get_child(p_array[2])
		except (KeyError,tables.NoSuchNodeError):
			return False

	def __create_partition(self,partition_dt):
		"""Creates partition, including parent groups (if they don't exist) and the data table
		"""

		p_array = self.__partition_date_to_path_array(partition_dt)
		
		# For each component, fetch the group or create it
		# Year
		try:
			y_group = self.root_group._f_get_child(p_array[0])
		except tables.NoSuchNodeError:
			y_group = self.file.create_group(self.root_group,p_array[0])

		# Month
		try:
			m_group = y_group._f_get_child(p_array[1])
		except tables.NoSuchNodeError:
			m_group = self.file.create_group(y_group,p_array[1])

		# Day
		try:
			d_group = m_group._f_get_child(p_array[2])
		except tables.NoSuchNodeError:
			d_group = self.file.create_group(m_group,p_array[2])

		# We need to create the table in the day group
		ts_data = self.file.create_table(d_group,'ts_data',self.table_description,self.table_title,
			self.table_filters, self.table_expectedrows, self.table_chunkshape, self.table_byteorder)

		# Need to save this as an attribute because it doesn't seem to be saved anywhere
		ts_data.attrs._TS_TABLES_EXPECTEDROWS_PER_PARTITION = self.table_expectedrows

		return ts_data

	def __fetch_or_create_partition_table(self,partition_dt):
		group = self.__fetch_partition_group(partition_dt)
		if group:
			return group._f_get_child('ts_data')
		else:
			return self.__create_partition(partition_dt)






		







	