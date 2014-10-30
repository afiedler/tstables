import tables
import tstables
import tempfile
import datetime
import pytz
import pandas
import numpy
import timeit
import os

# Class to define record structure
class Price(tables.IsDescription):
    timestamp = tables.Int64Col(pos=0)
    price = tables.Int32Col(pos=1)

class Benchmark:
    @classmethod
    def log_me(cls, s):
        cls.log.write(s)
        print(s) 
    
    @classmethod
    def write_data(cls):
        # This simple benchmark creates a HDF5 file with a timeseries. It then loads about one year of random secondly
        #  data into it, closes it, and reads it back.
   
        cls.log_me("Started benchmark at %s\n\n" % datetime.datetime.now())
    
        cls.temp_file = tempfile.mkstemp('h5')[1]
        cls.h5_file = tables.open_file(cls.temp_file,'r+')
        ts = cls.h5_file.create_ts('/','EURUSD',description=Price)
    
        start_dt = datetime.datetime(2014,1,1,tzinfo=pytz.utc)
    
        # period is number of seconds in 31 days.
        # will result in slightly more than a year of data.
        index = pandas.date_range(start_dt, periods=2678400, freq='S')
        values = numpy.int32(numpy.random.rand(2678400, 1)*numpy.iinfo(numpy.int32).max)
        df = pandas.DataFrame(values,index=index,columns=['price'],dtype=numpy.dtype('i4'))
    
        append_times = []
        for month in range(0,12):
            t = timeit.timeit(lambda: ts.append(df), number=1)
            df.index = df.index+pandas.offsets.Day(31) # Shift index to next month
            cls.log_me(" * finished appending month {0}\n".format(month))
            append_times.append(t)
    
        cls.log_me("Appended 12 months of data:\n")
        for a in append_times:
            cls.log_me(" * {0} seconds\n".format(a))
    
        cls.log_me("average {0} seconds, total {1} seconds\n\n".format(sum(append_times)/len(append_times),
                                                                   sum(append_times)))
    
    
        # Now, close the file and re-open it
        cls.h5_file.close()
        
    @classmethod
    def read_data(cls):
        # report the file size
        h5_size = os.stat(cls.temp_file).st_size
        cls.log_me("file size (bytes): {0}\n".format(h5_size))

        cls.h5_file = tables.open_file(cls.temp_file,'r')
        ts = cls.h5_file.root.EURUSD._f_get_timeseries()

        # Now, read random one hour increments

        def read_random_hour(ts,min_dt,max_dt):
            rnd = numpy.random.rand(1)[0]
            start_offset = datetime.timedelta(seconds=(max_dt - min_dt - datetime.timedelta(hours=1)).total_seconds() * rnd)
            start_dt = min_dt + start_offset
            end_dt = start_dt + datetime.timedelta(hours=1)

            ts.read_range(start_dt,end_dt)

        min_dt = ts.min_dt()
        max_dt = ts.max_dt()

        read_time = timeit.timeit(lambda: read_random_hour(ts, min_dt, max_dt), number=100)

        cls.log_me("average time to read one hour of data (100 repetitions): {0} seconds\n".format(read_time))


    @classmethod
    def main(cls):
        cls.log = open('benchmark.txt', 'w')
        
        cls.write_data()
        
        cls.read_data()

        # Finished!
        cls.log.close()
