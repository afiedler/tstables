from setuptools import setup, find_packages
setup(

    # Package structure
    #
    # find_packages searches through a set of directories 
    # looking for packages
    packages = find_packages('src', exclude = ['*.tests', '*.tests.*', 'tests.*', 'tests']),
    
    # package_dir directive maps package names to directories.
    # package_name:package_directory
    package_dir = {'': 'src'},

    # Not all packages are capable of running in compressed form, 
    # because they may expect to be able to access either source 
    # code or data files as normal operating system files.
    zip_safe = True,

    # Entry points
    #
    # install the executable
    entry_points = {
        #'console_scripts': ['helloworld = greatings.helloworld:main']
    },

    # Dependencies
    #
    # Dependency expressions have a package name on the left-hand 
    # side, a version on the right-hand side, and a comparison 
    # operator between them, e.g. == exact version, >= this version
    # or higher
    install_requires = ['tables>=3.1.1', 'pandas>=0.13.1'],

    # Tests
    #
    # Tests must be wrapped in a unittest test suite by either a
    # function, a TestCase class or method, or a module or package
    # containing TestCase classes. If the named suite is a package,
    # any submodules and subpackages are recursively added to the
    # overall test suite.
    test_suite = 'tstables.tests.suite',
    # Download dependencies in the current directory
    tests_require = 'docutils >= 0.6',

    name = "tstables",
    version = "0.0.2",

    # metadata for upload to PyPI
    author = "Andy Fiedler",
    author_email = "andy@andyfiedler.com",
    description = "Handles large time series using PyTables and Pandas",
    license = "MIT",
    keywords = "time series high frequency HDF5",
    url = "http://github.com/afiedler/tstables",   # project home page, if any

    # could also include long_description, download_url, classifiers, etc.
)