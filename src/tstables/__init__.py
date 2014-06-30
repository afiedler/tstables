# -*- coding: utf-8 -*-

########################################################################
#
# License: MIT
#
# $Id$
#
########################################################################

"""TsTables is a Python package to store time series data in HDF5 files using PyTables and Pandas

:URL: http://afiedler.github.io/tstables

TsTables is a Python package to store time series data in HDF5 files using PyTables. It stores time series data into
daily partitions and provides functions to query for subsets of data across partitions.

Its goals are to support a workflow where tons (gigabytes) of time series data are appended periodically to a HDF5 file,
and need to be read many times (quickly) for analytical models and research.

"""
from ._version import __version__
from tstables.tstable import TsTable
from tstables.file import create_ts
from tstables.group import timeseries_repr
from tstables.group import timeseries_str
from tstables.group import get_timeseries
from tstables.benchmark import Benchmark
import tables

# Augment the PyTables File class
tables.File.create_ts = create_ts

# Patch the group class to return time series __str__ and __repr__
old_repr = tables.Group.__repr__
old_str = tables.Group.__str__

tables.Group.__repr__ = timeseries_repr
tables.Group.__str__ = timeseries_str

# Add _v_timeseries to Group
tables.Group._f_get_timeseries = get_timeseries

