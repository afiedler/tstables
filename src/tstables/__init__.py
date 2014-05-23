# -*- coding: utf-8 -*-

########################################################################
#
# License: MIT
#
# $Id$
#
########################################################################

"""TsTables, very large time series with PyTables

:URL: http://tstables.github.io/

TsTables is a wrapper for PyTables that allows you to manage very large time series.

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

