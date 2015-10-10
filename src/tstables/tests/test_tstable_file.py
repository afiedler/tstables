import tables
import tstables
import unittest
import datetime
import pytz
import tempfile
try:
    from io import StringIO
except ImportError:
    from cStringIO import StringIO
import os
import pandas
import mock
import numpy

# Class to define record structure
class Price(tables.IsDescription):
    timestamp = tables.Int64Col(pos=0)
    price = tables.Int32Col(pos=1)


class TsTableFileTestCase(unittest.TestCase):

    def setUp(self):
        self.temp_file = tempfile.mkstemp('h5')[1]
        self.h5_file = tables.open_file(self.temp_file,'r+')

    def tearDown(self):
        self.h5_file.close()
        os.remove(self.temp_file)


    def test_create_ts(self):
        # Technically, there is a race condition here if you happen to run this at exactly midnight UTC!
        now = datetime.datetime.utcnow()
        self.h5_file.create_ts('/','EURUSD',description=Price)

        # Want to check that:
        # - the group exists
        # - it has a _TS_TABLES_CLASS attribute equal to "TIMESERIES"
        # - it has a table at yYYYY/mMM/dDD/ts_data, where YYY-MM-DD is today (in UTC)
        # - the dtype is correct
        self.assertEqual(self.h5_file.root.EURUSD.__class__, tables.Group)
        self.assertEqual(self.h5_file.root.EURUSD._v_attrs._TS_TABLES_CLASS,'TIMESERIES')

        path = tstables.TsTable._TsTable__partition_date_to_path_array(now.date())

        ts_data = self.h5_file.root.EURUSD._f_get_child(path[0])._f_get_child(path[1])._f_get_child(
            path[2])._f_get_child('ts_data')

        self.assertEqual(ts_data.attrs._TS_TABLES_EXPECTEDROWS_PER_PARTITION,10000)

        self.assertEqual(ts_data._v_dtype[0],tables.dtype_from_descr(Price)[0])
        self.assertEqual(ts_data._v_dtype[1],tables.dtype_from_descr(Price)[1])


    def test_create_ts_with_invalid_description_incorrect_order(self):
        class InvalidDesc(tables.IsDescription):
            # Positions are out of order here!
            timestamp = tables.Int64Col(pos=1)
            price = tables.Int32Col(pos=0)

        self.assertRaises(AttributeError, self.h5_file.create_ts, '/', 'EURUSD', description=InvalidDesc)

    def test_create_ts_with_invalid_description_incorrect_order(self):
        class InvalidDesc(tables.IsDescription):
            # Type is incorrect here!
            timestamp = tables.Int32Col(pos=0)
            price = tables.Int32Col(pos=1)

        self.assertRaises(AttributeError, self.h5_file.create_ts, '/', 'EURUSD', description=InvalidDesc)

    def test_load_same_timestamp(self):

        # Test data that is multiple rows with the same timestamp
        csv = u"""2014-05-05T01:01:01.100Z,1
                 2014-05-05T01:01:01.100Z,2
                 2014-05-05T01:01:01.100Z,3
                 2014-05-05T01:01:01.100Z,4
                 2014-05-05T01:01:01.100Z,5"""

        sfile = StringIO(csv)

        # Note: don't need the 'timestamp' column in the dtype param here because it will become the DatetimeIndex.
        rows = pandas.read_csv(sfile,parse_dates=[0],index_col=0,names=['timestamp', 'price'],dtype={'price': 'i4'})

        ts = self.h5_file.create_ts('/','EURUSD',description=Price)
        ts.append(rows)

        # Inspect to ensure that data has been stored correctly
        tbl = ts.root_group.y2014.m05.d05.ts_data

        self.assertEqual(tbl.nrows,5)

        # Fetch rows over a larger range
        rows_read = ts.read_range(datetime.datetime(2014,5,5,tzinfo=pytz.utc),datetime.datetime(2014,5,6,tzinfo=pytz.utc))

        # Confirm equality
        for idx,p in enumerate(rows_read['price']):
            self.assertEqual(p,rows['price'][idx])

        # Fetch rows over the smallest possible range
        rows_read = ts.read_range(datetime.datetime(2014,5,5,1,1,1,100*1000,tzinfo=pytz.utc),
                                  datetime.datetime(2014,5,5,1,1,1,100*1000,tzinfo=pytz.utc))

        # Confirm equality
        for idx,p in enumerate(rows_read['price']):
            self.assertEqual(p,rows['price'][idx])


    @mock.patch.object(tstables.TsTable, 'MAX_FULL_PARTITION_READ_SIZE', 1)
    def test_load_same_timestamp(self):

        # Test data that is multiple rows with the same timestamp
        csv = u"""2014-05-05T01:01:01.100Z,1
                 2014-05-05T01:01:01.100Z,2
                 2014-05-05T01:01:01.100Z,3
                 2014-05-05T01:01:01.100Z,4
                 2014-05-05T01:01:01.100Z,5"""

        sfile = StringIO(csv)

        # Note: don't need the 'timestamp' column in the dtype param here because it will become the DatetimeIndex.
        rows = pandas.read_csv(sfile,parse_dates=[0],index_col=0,names=['timestamp', 'price'],dtype={'price': 'i4'})

        ts = self.h5_file.create_ts('/','EURUSD',description=Price)
        ts.append(rows)

        # Inspect to ensure that data has been stored correctly
        tbl = ts.root_group.y2014.m05.d05.ts_data

        self.assertEqual(tbl.nrows,5)

        # Fetch rows over a larger range
        rows_read = ts.read_range(datetime.datetime(2014,5,5,tzinfo=pytz.utc),datetime.datetime(2014,5,6,tzinfo=pytz.utc))

        # Confirm equality
        for idx,p in enumerate(rows_read['price']):
            self.assertEqual(p,rows['price'][idx])

        # Fetch rows over the smallest possible range
        rows_read = ts.read_range(datetime.datetime(2014,5,5,1,1,1,100*1000,tzinfo=pytz.utc),
                                  datetime.datetime(2014,5,5,1,1,1,100*1000,tzinfo=pytz.utc))

        # Confirm equality
        for idx,p in enumerate(rows_read['price']):
            self.assertEqual(p,rows['price'][idx])


    @mock.patch.object(tstables.TsTable, 'MAX_FULL_PARTITION_READ_SIZE', 1)
    @mock.patch.object(tables.Table, 'read_where')
    @mock.patch.object(tables.Table, 'read')
    def test_read_using_read_where(self, mock_read, mock_read_where):

        csv = u"""2014-05-05T01:01:01.100Z,1
                 2014-05-05T01:01:01.100Z,2
                 2014-05-05T01:01:01.100Z,3
                 2014-05-05T01:01:01.100Z,4
                 2014-05-05T01:01:01.100Z,5"""

        sfile = StringIO(csv)

        # Note: don't need the 'timestamp' column in the dtype param here because it will become the DatetimeIndex.
        rows = pandas.read_csv(sfile,parse_dates=[0],index_col=0,names=['timestamp', 'price'],dtype={'price': 'i4'})

        ts = self.h5_file.create_ts('/','EURUSD',description=Price)
        ts.append(rows)

        # Inspect to ensure that data has been stored correctly
        tbl = ts.root_group.y2014.m05.d05.ts_data

        self.assertEqual(tbl.nrows,5)

        # Table.read_where is a mock, so we need to give it a return value
        mock_read_where.return_value = numpy.ndarray(shape=0,dtype=[('timestamp', '<i8'), ('price', '<i4')])

        # Fetch rows over a larger range
        rows_read = ts.read_range(datetime.datetime(2014,5,5,tzinfo=pytz.utc),datetime.datetime(2014,5,6,tzinfo=pytz.utc))

        self.assertEquals(mock_read_where.called, True)
        self.assertEquals(mock_read.called, False)

    @mock.patch.object(tables.Table, 'read_where')
    @mock.patch.object(tables.Table, 'read')
    def test_read_using_read_where(self, mock_read, mock_read_where):

        csv = u"""2014-05-05T01:01:01.100Z,1
                 2014-05-05T01:01:01.100Z,2
                 2014-05-05T01:01:01.100Z,3
                 2014-05-05T01:01:01.100Z,4
                 2014-05-05T01:01:01.100Z,5"""

        sfile = StringIO(csv)

        # Note: don't need the 'timestamp' column in the dtype param here because it will become the DatetimeIndex.
        rows = pandas.read_csv(sfile,parse_dates=[0],index_col=0,names=['timestamp', 'price'],dtype={'price': 'i4'})

        ts = self.h5_file.create_ts('/','EURUSD',description=Price)
        ts.append(rows)

        # Inspect to ensure that data has been stored correctly
        tbl = ts.root_group.y2014.m05.d05.ts_data

        self.assertEqual(tbl.nrows,5)

        # Table.read_where is a mock, so we need to give it a return value
        mock_read.return_value = numpy.ndarray(shape=0,dtype=[('timestamp', '<i8'), ('price', '<i4')])

        # Fetch rows over a larger range
        rows_read = ts.read_range(datetime.datetime(2014,5,5,tzinfo=pytz.utc),datetime.datetime(2014,5,6,tzinfo=pytz.utc))

        self.assertEquals(mock_read_where.called, False)
        self.assertEquals(mock_read.called, True)


    def __load_csv_data(self,csv):
        sfile = StringIO(csv)

        # Note: don't need the 'timestamp' column in the dtype param here because it will become the DatetimeIndex.
        rows = pandas.read_csv(sfile,parse_dates=[0],index_col=0,names=['timestamp', 'price'],dtype={'price': 'i4'})

        ts = self.h5_file.create_ts('/','EURUSD',description=Price)
        ts.append(rows)

        return ts,rows

    def test_load_cross_partition_boundary_timestamps(self):

        # This data should just cross the partition boundary between 5/4 and 5/5
        csv = u"""2014-05-04T23:59:59.998Z,1
                 2014-05-04T23:59:59.999Z,2
                 2014-05-04T23:59:59.999Z,3
                 2014-05-05T00:00:00.000Z,4
                 2014-05-05T00:00:00.001Z,5"""

        ts,rows = self.__load_csv_data(csv)

        # Inspect to ensure that data has been stored correctly
        tbl = ts.root_group.y2014.m05.d04.ts_data

        # Three rows on the 4th
        self.assertEqual(tbl.nrows,3)

        tbl = ts.root_group.y2014.m05.d05.ts_data

        # Two rows on the 5th
        self.assertEqual(tbl.nrows,2)

        # Fetch rows over a larger range
        rows_read = ts.read_range(datetime.datetime(2014,5,4,tzinfo=pytz.utc),datetime.datetime(2014,5,6,tzinfo=pytz.utc))

        # Confirm equality
        for idx,p in enumerate(rows_read['price']):
            self.assertEqual(p,rows['price'][idx])

        # Fetch rows over the smallest possible range
        rows_read = ts.read_range(datetime.datetime(2014,5,4,23,59,59,998*1000,tzinfo=pytz.utc),
                                  datetime.datetime(2014,5,5,0,0,0,1*1000,tzinfo=pytz.utc))

        # Confirm equality
        for idx,p in enumerate(rows_read['price']):
            self.assertEqual(p,rows['price'][idx])

    def test_read_data_end_date_before_start_date(self):
        csv = u"""2014-05-04T23:59:59.998Z,1
                 2014-05-04T23:59:59.999Z,2
                 2014-05-04T23:59:59.999Z,3
                 2014-05-05T00:00:00.000Z,4
                 2014-05-05T00:00:00.001Z,5"""

        ts,rows = self.__load_csv_data(csv)

        # Try to fetch with end_dt before start_dt
        end_dt = datetime.datetime(2014,5,1)
        start_dt = datetime.datetime(2014,5,5)
        self.assertRaises(AttributeError, ts.read_range, start_dt, end_dt)

        # This should work, and return just this row: '2014-05-05T00:00:00.000Z,4'
        end_dt = start_dt
        rng = ts.read_range(start_dt,end_dt)

        self.assertEqual(rng['price'].size, 1)
        self.assertEqual(rng['price'][0],4)

    def test_no_data_stored_in_missing_day(self):
        # Note that May 5 is missing
        csv = u"""2014-05-04T23:59:59.998Z,1
                 2014-05-04T23:59:59.999Z,2
                 2014-05-04T23:59:59.999Z,3
                 2014-05-06T00:00:00.000Z,4
                 2014-05-06T00:00:00.001Z,5"""

        ts,rows = self.__load_csv_data(csv)

        tbl = ts.root_group.y2014.m05.d05.ts_data

        # No rows on the 5th
        self.assertEqual(tbl.nrows,0)

        tbl = ts.root_group.y2014.m05.d06.ts_data

        # Two rows on the 6th
        self.assertEqual(tbl.nrows,2)

        tbl = ts.root_group.y2014.m05.d04.ts_data

        # Three rows on the 4th
        self.assertEqual(tbl.nrows,3)

    def test_exception_on_unsorted_data(self):
        # Note that this is unsorted
        csv = u"""2014-05-05T23:59:59.998Z,1
                 2014-05-04T23:59:59.999Z,2
                 2014-05-04T23:59:59.999Z,3
                 2014-05-06T00:00:00.000Z,4
                 2014-05-06T00:00:00.001Z,5"""

        self.assertRaises(ValueError, self.__load_csv_data, csv)

    def test_append_no_data(self):
        # No data, just making sure this doesn't throw an exception or anything
        csv = u""""""

        ts,rows = self.__load_csv_data(csv)

        self.assertEqual(rows['price'].size, 0)

    def test_no_group_created_on_create_ts_exception(self):
        self.assertRaises(ValueError,self.h5_file.create_ts,'/','EURUSD',description=Price,
                          chunkshape='an invalid chunkshape')

        # Should not have created the group
        self.assertRaises(tables.NoSuchNodeError,self.h5_file.root._f_get_child,'EURUSD')



def suite():
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(TsTableFileTestCase))
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())