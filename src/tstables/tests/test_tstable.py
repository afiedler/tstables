import tstables
import unittest
import datetime
import pytz

class TsTableTestCase(unittest.TestCase):

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


def suite():
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(TsTableTestCase))
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())