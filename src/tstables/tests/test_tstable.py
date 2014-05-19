import tables
import tstables
import unittest
import datetime
import pytz
import tempfile
import os

class TsTableTestCase(unittest.TestCase):

    #
    # Static and class methods (don't need a HDF5 file open to test)
    #

    def test_partition_range_same_time(self):
        # 2014-04-01 01:00:00 UTC
        start_dt = datetime.datetime(2014,4,1,1,0,tzinfo=pytz.utc)

        # End at the exact same time
        end_dt = start_dt
        parts = tstables.TsTable._TsTable__dtrange_to_partition_ranges(start_dt,end_dt)

        # There should be only one partition, and the range should equal start_dt,end_dt (the same values)
        assert parts[start_dt.date()] == (start_dt,end_dt)
        assert len(parts.keys()) == 1

    def test_partition_range_same_day(self):
        # 2014-04-01 01:00:00 UTC
        start_dt = datetime.datetime(2014,4,1,1,0,tzinfo=pytz.utc)

        # 2014-04-01 04:00:00 UTC
        end_dt = datetime.datetime(2014,4,1,4,0,tzinfo=pytz.utc)
        parts = tstables.TsTable._TsTable__dtrange_to_partition_ranges(start_dt,end_dt)

        # There should be only one partition, and the range should equal start_dt,end_dt
        assert parts[start_dt.date()] == (start_dt,end_dt)
        assert len(parts.keys()) == 1

    def test_partition_range_two_day(self):
        # 2014-04-01 01:00:00 UTC
        start_dt = datetime.datetime(2014,4,1,1,0,tzinfo=pytz.utc)

        # 2014-04-02 04:00:00 UTC
        end_dt = datetime.datetime(2014,4,2,4,0,tzinfo=pytz.utc)
        parts = tstables.TsTable._TsTable__dtrange_to_partition_ranges(start_dt,end_dt)

        # Should be two parts: [2014-04-01 01:00:00 UTC, 2014-04-02 00:00:00 UTC) and
        # [2014-04-02 01:00:00 UTC, 2014-04-02 04:00:00 UTC) 
        assert parts[start_dt.date()][0] == start_dt
        assert parts[start_dt.date()][1] == datetime.datetime(2014,4,2,0,0,tzinfo=pytz.utc)
        assert parts[end_dt.date()][0] == datetime.datetime(2014,4,2,0,0,tzinfo=pytz.utc)
        assert parts[end_dt.date()][1] == end_dt 
        assert len(parts.keys()) == 2

    def test_partition_range_three_day(self):
        # 2014-04-01 01:00:00 UTC
        start_dt = datetime.datetime(2014,4,1,1,0,tzinfo=pytz.utc)

        # 2014-04-03 04:00:00 UTC
        end_dt = datetime.datetime(2014,4,3,4,0,tzinfo=pytz.utc)
        parts = tstables.TsTable._TsTable__dtrange_to_partition_ranges(start_dt,end_dt)

        # Should be three parts: [2014-04-01 01:00:00 UTC, 2014-04-02 00:00:00 UTC),
        # [2014-04-02 00:00:00 UTC, 2014-04-03 00:00:00 UTC]
        # [2014-04-03 01:00:00 UTC, 2014-04-03 04:00:00 UTC) 
        assert parts[start_dt.date()][0] == start_dt
        assert parts[start_dt.date()][1] == datetime.datetime(2014,4,2,0,0,tzinfo=pytz.utc)

        mid_date = start_dt.date() + datetime.timedelta(days=1)
        assert parts[mid_date][0] == datetime.datetime(2014,4,2,0,0,tzinfo=pytz.utc)
        assert parts[mid_date][1] == datetime.datetime(2014,4,3,0,0,tzinfo=pytz.utc)

        assert parts[end_dt.date()][0] == datetime.datetime(2014,4,3,0,0,tzinfo=pytz.utc)
        assert parts[end_dt.date()][1] == end_dt 
        assert len(parts.keys()) == 3

    def test_dt_to_ts(self):
        dt = datetime.datetime(1970,1,1,tzinfo=pytz.utc)
        ts = tstables.TsTable._TsTable__dt_to_ts(dt)

        assert ts == 0

        dt = datetime.datetime(1971,1,1,tzinfo=pytz.utc)
        ts = tstables.TsTable._TsTable__dt_to_ts(dt)

        assert ts == 31536000000

    def test_ts_to_dt(self):
        ts = 0
        dt = tstables.TsTable._TsTable__ts_to_dt(ts)

        assert dt == datetime.datetime(1970,1,1,tzinfo=pytz.utc)

        ts = 31536000000
        dt = tstables.TsTable._TsTable__ts_to_dt(ts)

        assert dt == datetime.datetime(1971,1,1,tzinfo=pytz.utc)

    def test_partition_date_to_path_array(self):
        dt = datetime.datetime(2014,5,5,1,1,1,tzinfo=pytz.utc)
        pa = tstables.TsTable._TsTable__partition_date_to_path_array(dt)
        expected = ['y2014','m05','d05']
        for idx,p in enumerate(pa):
            assert p == expected[idx]

    #
    # End static and class methods. Now need to test things that write to H5 files
    #

    def test_create_ts(self):
        class Price(tables.IsDescription):
            timestamp = tables.Int64Col()
            price = tables.Int32Col()

        # Technically, there is a race condition here if you happen to run this at exactly midnight UTC!
        now = datetime.datetime.utcnow()
        self.h5_file.create_ts('/','EURUSD',description=Price)

        # Want to check that:
        # - the group exists
        # - it has a _TS_TABLES_CLASS attribute equal to "TIMESERIES"
        # - it has a table at yYYYY/mMM/dDD/ts_data, where YYY-MM-DD is today (in UTC)
        self.assertEquals(self.h5_file.root.EURUSD.__class__, tables.Group)
        self.assertEquals(self.h5_file.root.EURUSD._v_attrs._TS_TABLES_CLASS,'TIMESERIES')

        path = tstables.TsTable._TsTable__partition_date_to_path_array(now.date())

        ts_data = self.h5_file.root.EURUSD._f_get_child(path[0])._f_get_child(path[1])._f_get_child(
            path[2])._f_get_child('ts_data')

        self.assertEquals(ts_data.attrs._TS_TABLES_EXPECTEDROWS_PER_PARTITION,10000)

    def setUp(self):
        self.temp_file = tempfile.mkstemp('h5')[1]
        self.h5_file = tables.open_file(self.temp_file,'r+')

    def tearDown(self):
        self.h5_file.close()
        os.remove(self.temp_file)


def suite():
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(TsTableTestCase))
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())