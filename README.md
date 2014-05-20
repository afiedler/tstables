# TsTables

TsTables is a Python package to store time series data in HDF5 files using PyTables. It stores time
series data into daily partitions and provides functions to query for subsets of data across
partitions.

Its goals are to support a workflow where tons (gigabytes) of time series data are 
appended periodically to a HDF5 file, and need to be read many times (quickly) for analytical models
and research.

## Example

This example reads in minutely bitcoin price data and then fetches a range of data. For the full example here, and other
examples, see [EXAMPLES.md](EXAMPLES.md).

```python
# Class to use as the table description
class BpiValues(tables.IsDescription):
    timestamp = tables.Int64Col(pos=0)
    bpi = tables.Float64Col(pos=1)

# Use pandas to read in the CSV data
bpi = pandas.read_csv('bpi_2014_01.csv',index_col=0,names=['date','bpi'],parse_dates=True)

f = tables.open_file('bpi.h5','a')

# Create a new time series
ts = f.create_ts('/','BPI',BpiValues)

# Append the BPI data
ts.append(bpi)

# Read in some data
read_start_dt = datetime(2014,1,4,12,00)
read_end_dt = datetime(2014,1,4,14,30)

rows = ts.read_range(read_start_dt,read_end_dt)

# `rows` will be a pandas DataFrame with a DatetimeIndex.
```

## Pre-release software

TsTables is currently under development and has yet to be used extensively in production. It is reaching the point where
it is reasonably well-tested, so if you'd like to use it, feel free! If you are interested in the project (to contribute
or to hear about updates), email Andy Fiedler at <andy@andyfiedler.com>.
