import tstables
import unittest
import datetime
import pytz

class TsTableStaticTestCase(unittest.TestCase):

    TIME_EPS = datetime.timedelta(microseconds=1*1000)

    def test_partition_range_same_time(self):
        # 2014-04-01 01:00:00 UTC
        start_dt = datetime.datetime(2014,4,1,1,0,tzinfo=pytz.utc)

        # End at the exact same time
        end_dt = start_dt
        parts = tstables.TsTable._TsTable__dtrange_to_partition_ranges(start_dt,end_dt)

        # There should be only one partition, and the range should equal start_dt,end_dt
        self.assertEqual(parts[start_dt.date()], (start_dt,end_dt))
        self.assertEqual(len(parts.keys()), 1)

    def test_partition_range_same_day(self):
        # 2014-04-01 01:00:00 UTC
        start_dt = datetime.datetime(2014,4,1,1,0,tzinfo=pytz.utc)

        # 2014-04-01 04:00:00 UTC
        end_dt = datetime.datetime(2014,4,1,4,0,tzinfo=pytz.utc)
        parts = tstables.TsTable._TsTable__dtrange_to_partition_ranges(start_dt,end_dt)

        # There should be only one partition, and the range should equal start_dt,end_dt + 1ms
        self.assertEqual(parts[start_dt.date()], (start_dt,end_dt))
        self.assertEqual(len(parts.keys()),1)

    def test_partition_range_two_day(self):
        # 2014-04-01 01:00:00 UTC
        start_dt = datetime.datetime(2014,4,1,1,0,tzinfo=pytz.utc)

        # 2014-04-02 04:00:00 UTC
        end_dt = datetime.datetime(2014,4,2,4,0,tzinfo=pytz.utc)
        parts = tstables.TsTable._TsTable__dtrange_to_partition_ranges(start_dt,end_dt)

        # Should be two parts: [2014-04-01 01:00:00 UTC, 2014-04-01 23:59:59.999 UTC] and
        # [2014-04-02 00:00:00 UTC, 2014-04-02 04:00:00 UTC]
        self.assertEqual(parts[start_dt.date()][0], start_dt)
        self.assertEqual(parts[start_dt.date()][1], datetime.datetime(2014,4,2,0,0,tzinfo=pytz.utc) - self.TIME_EPS)
        self.assertEqual(parts[end_dt.date()][0], datetime.datetime(2014,4,2,0,0,tzinfo=pytz.utc))
        self.assertEqual(parts[end_dt.date()][1],end_dt)
        self.assertEqual(len(parts.keys()),2)

    def test_partition_range_three_day(self):
        # 2014-04-01 01:00:00 UTC
        start_dt = datetime.datetime(2014,4,1,1,0,tzinfo=pytz.utc)

        # 2014-04-03 04:00:00 UTC
        end_dt = datetime.datetime(2014,4,3,4,0,tzinfo=pytz.utc)
        parts = tstables.TsTable._TsTable__dtrange_to_partition_ranges(start_dt,end_dt)

        # Should be three parts: [2014-04-01 01:00:00 UTC, 2014-04-01 23:59:59.999 UTC],
        # [2014-04-02 00:00:00 UTC, 2014-04-02 23:59:59.999 UTC]
        # [2014-04-03 00:00:00 UTC, 2014-04-03 04:00:00 UTC]
        self.assertEqual(parts[start_dt.date()][0], start_dt)
        self.assertEqual(parts[start_dt.date()][1], datetime.datetime(2014,4,2,0,0,tzinfo=pytz.utc)-self.TIME_EPS)

        mid_date = start_dt.date() + datetime.timedelta(days=1)
        self.assertEqual(parts[mid_date][0], datetime.datetime(2014,4,2,0,0,tzinfo=pytz.utc))
        self.assertEqual(parts[mid_date][1], datetime.datetime(2014,4,3,0,0,tzinfo=pytz.utc)-self.TIME_EPS)

        self.assertEqual(parts[end_dt.date()][0], datetime.datetime(2014,4,3,0,0,tzinfo=pytz.utc))
        self.assertEqual(parts[end_dt.date()][1], end_dt)
        self.assertEqual(len(parts.keys()),3)

    def test_partition_range_just_cross_boundary(self):
        # 2014-03-31 23:59:59.999 UTC
        start_dt = datetime.datetime(2014,3,31,23,59,59,999*1000,tzinfo=pytz.utc)

        # 2014-04-01 00:00:00.001 UTC
        end_dt = datetime.datetime(2014,4,1,0,0,0,1*1000,tzinfo=pytz.utc)
        parts = tstables.TsTable._TsTable__dtrange_to_partition_ranges(start_dt,end_dt)

        # Should be two parts: [2014-03-31 23:59:59.999 UTC, 2014-03-31 23:59:59.999 UTC] and
        # [2014-04-01 00:00:00 UTC, 2014-04-01 00:00:00.001 UTC]
        self.assertEqual(parts[start_dt.date()][0], start_dt)
        self.assertEqual(parts[start_dt.date()][1], datetime.datetime(2014,4,1,0,0,tzinfo=pytz.utc) - self.TIME_EPS)
        self.assertEqual(parts[end_dt.date()][0], datetime.datetime(2014,4,1,0,0,tzinfo=pytz.utc))
        self.assertEqual(parts[end_dt.date()][1], end_dt)
        self.assertEqual(len(parts.keys()),2)

    def test_dt_to_ts(self):
        # Test 1 - Epoch
        dt = datetime.datetime(1970,1,1,tzinfo=pytz.utc)
        ts = tstables.TsTable._TsTable__dt_to_ts(dt)

        self.assertEqual(ts, 0)

        # Test 2 - 1970-01-01T00:00:00.000
        dt = datetime.datetime(1971,1,1,tzinfo=pytz.utc)
        ts = tstables.TsTable._TsTable__dt_to_ts(dt)

        assert ts == 31536000000

        # Test 3 - 2014-05-05T01:01:01.100
        dt = datetime.datetime(year=2014,month=5,day=5,hour=1,minute=1,second=1,microsecond=100*1000,tzinfo=pytz.utc)
        ts = tstables.TsTable._TsTable__dt_to_ts(dt)

        assert ts == 1399251661100

    def test_ts_to_dt(self):
        # Test 1 - Epoch
        ts = 0
        dt = tstables.TsTable._TsTable__ts_to_dt(ts)

        assert dt == datetime.datetime(1970,1,1,tzinfo=pytz.utc)

        # Test 2 - 1970-01-01T00:00:00.000
        ts = 31536000000
        dt = tstables.TsTable._TsTable__ts_to_dt(ts)

        assert dt == datetime.datetime(1971,1,1,tzinfo=pytz.utc)

        # Test 3 - 2014-05-05T01:01:01.100
        ts = 1399251661100
        dt = tstables.TsTable._TsTable__ts_to_dt(ts)

        assert dt == datetime.datetime(year=2014,month=5,day=5,hour=1,minute=1,second=1,microsecond=100*1000,tzinfo=pytz.utc)

    def test_partition_date_to_path_array(self):
        dt = datetime.datetime(2014,5,5,1,1,1,tzinfo=pytz.utc)
        pa = tstables.TsTable._TsTable__partition_date_to_path_array(dt)
        expected = ['y2014','m05','d05']
        for idx,p in enumerate(pa):
            assert p == expected[idx]

def suite():
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(TsTableStaticTestCase))
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())