radical.saga unit test framework
================================

The test suites expects the python environment to be set up in a way that the
sage module is automatically found.  Also, it needs the ``nose`` module
installed (``pip install nose``).

The  ``run_tests.py`` script runs a set of unit tests. Tests are configured via 
config files. A set of config files are available in the subdirectory configs/.

To test one specific configuration, run the script with the .cfg as parameter,
e.g.:

```
./run_tests.py --config=test_internal.cfg
```

If you want to run multiple configurations, pass them as comma-separated list, 
e.g.:

```
./run_tests.py --config=test_internal.cfg,test_ssh_local.cfg
```

If you wnat to run all configurations, pass the directory as parameter, e.g.:

```
./run_tests.py --config=configs/
```

The verbosity of the test output can be set via the ``NOSE_VERBOSE`` 
environment variavble.

By default, the test suite handles ``saga.NotImplemented`` exceptions as errors.
If you set the ``--notimpl-warn-only`` flag, saga.NotImplemented exceptions
will still be reported, but they won't cause an error in the test suite.

Test configuration files
------------------------

A set of config files in ``configs/*.cfg`` are used to configure how the
individual test suites are run.

The config files are in particular used to accomodate remote unit testing, i.e.
to run the unit tests against arbitrary remote backends.  An example config file
for a job adaptor looks like this::

    [saga.tests]
    test_suites        = api/job
    job_service_url    = sge+gsissh://lonestar.tacc.utexas.edu
    job_queue          = normal
    job_walltime_limit = 4
    job_project        = TG-XXXXXXXXX

    context_type       =
    context_user_id    =
    context_user_pass  = 
    context_user_proxy = 
    context_user_cert  = 


An example config file for a job adaptor looks like this::

    [saga.tests]
    test_suites        = api/filesystem

   
