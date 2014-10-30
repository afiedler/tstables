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

Here is how to open a pre-existing `bpi.h5` HDF5 file and get that timeseries from it.

```python
f = tables.open_file('bpi.h5','r')
ts = f.root.BPI._f_get_timeseries()

# Read in some data
read_start_dt = datetime(2014,1,4,12,00)
read_end_dt = datetime(2014,1,4,14,30)

rows = ts.read_range(read_start_dt,read_end_dt)
```

## Running unit tests

You can run the unit test suite from the command line at the root of the repository:

`python setup.py test`


## Preliminary benchmarks

The main goal of TsTables is to make it very fast to read subsets of data, given a date range. TsTables currently
includes a simple benchmark to track progress towards that goal. To run it, after installing the package, you can run 
`tstables_benchmark` from the command line or you can import the package in a Python console and run it directly.

```python
import tstables
tstables.Benchmark.main()
```
    
Running the benchmark both prints results out to the screen and saves them in `benchmark.txt`.

The benchmark loads one year of random secondly data (just the timestamp column and a 32-bit integer "price" column) 
into a file, and then it reads random one hour chunks of data.

Currently, here's some benchmarks of TsTables (from a MacBook Pro with a SSD):

Metric                                                      | Results
------------------------------------------------------------|-----------------
Append one month of data (2.67 million rows)                | 0.711 seconds
Fetch one hour of data into memory                          | 0.305 seconds
File size (one year of data, 32 million rows, uncompressed) | 391.6 MB

HDF5 supports zlib and other compression algorithms, which can be enabled through PyTables to reduce the file 
size. Without compression, the HDF5 file size is approximately 1.8% larger than the raw data in binary form, a 
drastically lower overhead than CSV files.

## Contributing

If you are interested in the project (to contribute
or to hear about updates), email Andy Fiedler at <andy@andyfiedler.com> or submit a pull request.
