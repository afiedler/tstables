import pytz
import datetime
import tables
import numpy

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
	
	def __dt_to_ts(dt):
		delta = dt - self.EPOCH
		ts = numpy.int64(delta.total_seconds()) # This will strip off fractional seconds
		ts = ts * 1000 # shift to milliseconds
		ts = ts + numpy.int64(delta.microseconds/1000)
		return ts

	def __ts_to_dt(ts):
		# Trying to avoid a lossy conversion here. If we were to cast the ts as a timedelta using
		# just milliseconds, we might overflow the buffer
		ts_milliseconds = ts % 1000
		ts_seconds = ts - ts_milliseconds
		dt = self.__offset + datetime.timedelta(seconds=ts_seconds)
		dt = dt + datetime.timedelta(microseconds=ts_milliseconds*1000)
		return dt


	def __fetch_rows_from_partition(partition_date,start_dt,end_dt):
		try:
			y_group = self.root_group._v_groups[partition_date.strftime('y%Y')]
			m_group = y_group._v_groups[partition_date.strftime('m%m')]
			d_group = m_group._v_groups[partition_date.strftime('d%d')]
		except KeyError:
			# If the partition group is missing, then return an empty array
			return []

		return d_group.tsdata.read_where('(timestamp >= {0}) & (timestamp < {1})'.format(
			self.__dt_to_ts(start_dt),self.__dt_to_ts(end_dt)))

	def read_range(start_dt,end_dt):
		partitions = self.__dtrange_to_partition_ranges(start_dt,end_dt)
		# TODO: finish this!

	def append(rows):
		# Try to convert the object into a recarray compliant with table. This code is stolen from
		# PyTable's append method.
		try:
			iflavor = tables.flavor_of(rows)
			if iflavor != 'python':
				rows = tables.array_as_internal(rows,iflavor)
			wbufRA = numpy.rec.array(rows, dtype=self._v_dtype)
		except Exception as exc:
			raise ValueError("rows parameter cannot be converted into a recarray object compliant "
							 "with table '%s'.  The error was: <%s>" % (str(self), exc))

		# Confirm that first column is Int64. This is an additional constraint of TsTables.		
		if not type(wbufRA[0,0]) == 'numpy.int64':
			raise ValueError("first column must be of type numpy.int64.")

		# We also need to confirm that the rows are sorted by timestamp. This is an additional
		# constraint of TsTables.
		prev_ts = numpy.iinfo('int64').min
		for ts in wbufRA[:,0]: # probably not ideal to loop here. is there a faster way?
			if ts < prev_ts:
				raise ValueError("timestamp column must be sorted in ascending order.")

		# Array is sorted at this point, so min and max are easy to get
		min_ts = wbufRA[0,0]
		max_ts = wbufRA[-1,0]

		# Confirm that min is >= to the TsTable's max_ts
		if min_ts < self.max_ts:
			raise ValueError("rows start prior to the end of existing rows, so they cannot be "
							 "appended.")

		# wbufRA is ready to be inserted at this point. Chop it up into partitions.
		min_dt = self.__ts_to_dt(min_ts)
		max_dt = self.__ts_to_dt(max_ts)
		possible_partitions = self.__dtrange_to_partition_ranges(min_dt,max_dt)

		# For each partition...(go in sorted order)
		for p in sorted(possible_partitions.keys()):
			rng = possible_partitions[p]
			start_ts = self.__dt_to_ts(rng[0])
			end_ts = self.__dt_to_ts(rng[1])
			bool_include_rows = (rows[:,0] >= start_ts) < end_ts

			# This subsets the rows and does the appending
			self.__append_rows_to_partition(p,rows[bool_include_rows,:])


	def __partition_date_to_path_array(partition_dt):
		"""Converts a partition date to an array of partition names
		"""

		return [partition_dt.strftime('y%Y'),partition_dt.strftime('m%m'),partition_dt.strftime('d%d')]

	def __append_rows_to_partition(partition_dt,rows):
		"""Appends rows to a partition (which might not exist yet, and will then be created)

		The rows argument is assumed to be sorted and *only* contain rows that have timestamps that
		are valid for this partition.
		"""

		ts_data = self.__fetch_or_create_partition_table(partition_dt)
		ts_data.append(rows)
	
	def __fetch_partition_group(partition_dt):
		"""Fetches a partition group, or returns `False` if the partition group does not exist
		"""

		try:
			p_array = self.__partition_date_to_path_array(partition_dt)
			return self.root_group._f_get_child(p_array[0])._f_get_child(p_array[1])._f_get_child(p_array[2])
		except KeyError:
			return False

	def __create_partition(partition_dt):
		"""Creates partition, including parent groups (if they don't exist) and the data table
		"""

		p_array = self.__partition_date_to_path_array(partition_dt)
		
		# For each component, fetch the group or create it
		# Year
		try:
			y_group = self.root_group._f_get_child(p_array[0])
		except NoSuchNodeError:
			y_group = self.file.create_group(self.root_group,p_array[0])

		# Month
		try:
			m_group = y_group._f_get_child(p_array[1])
		except NoSuchNodeError:
			m_group = self.file.create_group(y_group,p_array[1])

		# Day
		try:
			d_group = m_group._f_get_child(p_array[2])
		except NoSuchNodeError:
			d_group = self.file.create_group(m_group,p_array[2])

		# We need to create the table in the day group
		ts_data = self.file.create_table(d_group,'ts_data',self.table_description,self.table_title,
			self.table_filters, self.table_expectedrows, self.table_chunkshape, self.table_byteorder)

		return ts_data

	def __fetch_or_create_partition_table(partition_dt):
		group = self.__fetch_partition_group(partition_dt)
		if group:
			return group._f_get_child('ts_data')
		else:
			return self.__create_partition(partition_dt)






		







	