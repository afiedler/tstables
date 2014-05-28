import pytz
import datetime
import tables
import numpy
import numpy.lib.recfunctions
import pandas
import re

class TsTable:
    EPOCH = datetime.datetime(1970,1,1,tzinfo=pytz.utc)

    # Partition size is one day (in milliseconds)
    PARTITION_SIZE = numpy.int64(86400000)

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

    @classmethod
    def __tsrange_to_partition_ranges(self,start_ts,end_ts):
        start_partition = start_ts // self.PARTITION_SIZE
        end_partition = end_ts // self.PARTITION_SIZE

        # Handle the special case when there is only one partition
        if start_partition == end_partition:
            return {start_partition: (start_ts, end_ts)}

        partition_ranges = {}
        for p in range(start_partition,end_partition+1):
            if p == start_partition:
                # append truncated range from start_ts to the end of the partition
                partition_ranges[p] = tuple((start_ts, (start_partition+1)*self.PARTITION_SIZE-1))
            elif p == end_partition:
                # append truncated range from start of the partition to end_ts
                partition_ranges[p] = tuple(((end_partition*self.PARTITION_SIZE), end_ts))
            else:
                partition_ranges[p] = tuple((p*self.PARTITION_SIZE, (p+1)*self.PARTITION_SIZE - 1))

        return partition_ranges



    @classmethod
    def __dtrange_to_partition_ranges(self,start_dt,end_dt):

        # We assume that start_dt and end_dt are in UTC at this point
        start_ts = self.__dt_to_ts(start_dt)
        end_ts = self.__dt_to_ts(end_dt)

        ts_partitions = self.__tsrange_to_partition_ranges(start_ts,end_ts)

        dt_partitions = {}

        for k in ts_partitions.keys():
            day = self.__ts_to_dt(k*self.PARTITION_SIZE).date()
            s_ts = ts_partitions[k][0]
            e_ts = ts_partitions[k][1]
            dt_partitions[day] = tuple((self.__ts_to_dt(s_ts),self.__ts_to_dt(e_ts)))

        return dt_partitions
    
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
        ts_seconds = numpy.int64(numpy.divide((ts - (ts % 1000)) % 86400000,1000))
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

        # It is faster to fetch the entire partition into memory and process it with NumPy than to use Table.read_where
        p_data = d_group.ts_data.read()
        start_ts = self.__dt_to_ts(start_dt)
        end_ts = self.__dt_to_ts(end_dt)
        start_idx = numpy.searchsorted(p_data['timestamp'], start_ts, side='left')
        end_idx = numpy.searchsorted(p_data['timestamp'], end_ts, side='right')

        return p_data[start_idx:end_idx]

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

    def min_dt(self):
        return self.__ts_to_dt(self.__get_min_ts())

    def max_dt(self):
        return self.__ts_to_dt(self.__get_min_ts())

    def read_range(self,start_dt,end_dt,as_pandas_dataframe=True):
        # Convert start_dt and end_dt to UTC if they are naive
        if start_dt.tzinfo is None:
            start_dt = pytz.utc.localize(start_dt)
        if end_dt.tzinfo is None:
            end_dt = pytz.utc.localize(end_dt)


        if start_dt > end_dt:
            raise AttributeError('start_dt must be <= end_dt')
        

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

    def append(self,rows,convert_strings=False):
        # This part is specific to pandas support. If rows is a pandas DataFrame, convert it to a
        # format suitable to PyTables
        if rows.__class__ == pandas.core.frame.DataFrame:
            if rows.empty:
                return # Do nothing if we are appending nothing
            if rows.index.__class__ != pandas.tseries.index.DatetimeIndex:
                raise ValueError('when rows is a DataFrame, the index must be a DatetimeIndex.')

            # Convert to records
            records = rows.to_records(index=True,convert_datetime64=False)

            # Need to make two type conversions:
            # 1. Pandas stores strings internally as variable-length strings, which are converted to objects in NumPy
            #    PyTables can't store those in a StringCol, so this converts to fixed-length strings if convert_strings
            #    set to True.
            # 2. Need to convert the timestamp to datetime64[ms] (milliseconds)

            dest_dtype = self.__fetch_first_table().description._v_dtype

            new_descr = []
            existing_descr = records.dtype.descr

            for idx,d in enumerate(existing_descr):
                if existing_descr[idx][1] == '|O8' and dest_dtype[idx].char == 'S' and convert_strings:
                    # records dtype is something like |O8 and dest dt is a string
                    new_descr.append((existing_descr[idx][0], dest_dtype[idx]))
                elif idx == 0:
                    # Make sure timestamp is in milliseconds
                    new_descr.append((existing_descr[idx][0], '<M8[ms]'))
                else:
                    new_descr.append(existing_descr[idx])

            # recast to the new type
            rows = records.astype(numpy.dtype(new_descr))

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
        if not (numpy.diff(wbufRA['timestamp']) >= 0).all():
            raise ValueError("timestamp column must be sorted in ascending order.")

        # Array is confirmed sorted at this point, so min and max are easy to get
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

        # For each partition, we are splitting on the end date
        split_on_idx = []
        for p in sorted_pkeys:
            # p_max_ts is the maximum value of the timestamp column that SHOULD be included in this
            # partition.
            # We need to determine the row index of the row AFTER the last row where p_max_ts is <= to
            # the timestamp.
            p_max_ts = self.__dt_to_ts(possible_partitions[p][1])
            split_on = numpy.searchsorted(wbufRA['timestamp'], p_max_ts, side='right')
            split_on_idx.append(split_on)

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






        







    