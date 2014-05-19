import tables
import tstables
import unittest
import datetime
import pytz
import tempfile
import io
import os
import pandas

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
        self.assertEqual(self.h5_file.root.EURUSD.__class__, tables.Group)
        self.assertEqual(self.h5_file.root.EURUSD._v_attrs._TS_TABLES_CLASS,'TIMESERIES')

        path = tstables.TsTable._TsTable__partition_date_to_path_array(now.date())

        ts_data = self.h5_file.root.EURUSD._f_get_child(path[0])._f_get_child(path[1])._f_get_child(
            path[2])._f_get_child('ts_data')

        self.assertEqual(ts_data.attrs._TS_TABLES_EXPECTEDROWS_PER_PARTITION,10000)

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
        csv = """2014-05-05T01:01:01.100Z,1
                 2014-05-05T01:01:01.100Z,2
                 2014-05-05T01:01:01.100Z,3
                 2014-05-05T01:01:01.100Z,4
                 2014-05-05T01:01:01.100Z,5"""

        sfile = io.StringIO(csv)

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

    def test_load_cross_partition_boundary_timestamps(self):

        # This data should just cross the partition boundary between 5/4 and 5/5
        csv = """2014-05-04T23:59:59.998Z,1
                 2014-05-04T23:59:59.999Z,2
                 2014-05-04T23:59:59.999Z,3
                 2014-05-05T00:00:00.000Z,4
                 2014-05-05T00:00:00.001Z,5"""

        sfile = io.StringIO(csv)

        # Note: don't need the 'timestamp' column in the dtype param here because it will become the DatetimeIndex.
        rows = pandas.read_csv(sfile,parse_dates=[0],index_col=0,names=['timestamp', 'price'],dtype={'price': 'i4'})

        ts = self.h5_file.create_ts('/','EURUSD',description=Price)
        ts.append(rows)

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














def suite():
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(TsTableFileTestCase))
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())