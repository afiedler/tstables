# TsTables Examples

This document shows you a few examples of how to use TsTables to store and access data.

## Basic Examples

### Fetch the daily EURUSD exchange rate from FRED (Federal Reserve Economic Data)

This example fetches the daily EURUSD exchange rate from FRED. TsTables isn't really designed for 
storing daily data, but this simple example illustrates how you can get a Pandas DataFrame and 
append it to a time series.

```python
import tables
import tstables
import pandas.io.data as web
from datetime import *
import numpy

# Create a class to describe the table structure. The column "timestamp" is required, and must be
# in the first position (pos=0) and have the type Int64.
class prices(tables.IsDescription):
    timestamp = tables.Int64Col(pos=0)
    price = tables.Float64Col(pos=1)

f = tables.open_file('eurusd.h5','a')

# This creates the time series, which is just a group called 'EURUSD' in the root of the HDF5 file.
ts = f.create_ts('/','EURUSD',prices)

start = datetime(2010,1,1)
end = datetime(2014,5,2)

euro = web.DataReader("DEXUSEU", "fred", start, end)
ts.append(euro)
f.flush() 

# Now, read in a month of data
read_start_dt = datetime(2014,1,1)
read_end_dt = datetime(2014,1,31)

jan = ts.read_range(read_start_dt,read_end_dt)
```