saga-python unit test framework
===============================

The  run_tests.py script runs a set of unit tests. Tests are configured via 
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

The test suites expects the python environment to be set up in a way that the
sage module is automatically found.  Also, it needs the ``nose`` module
installed (``pip install nose``), which provides the testing framework.

A set of config files in ``configs/*.cfg`` are used to configure how the
individual test suites are run.

The config files are in particular used to accomodate remote unit testing, i.e.
to run the unit tests against arbitrary remote backends.  An example config file
is::

    # this config file will run the job package unit tests against
    # over a local ssh connection

    [saga.tests]
    test_suites        = engine,api/job

    job_service_url    = ssh://localhost/
    filesystem_url     =
    replica_url        =
    advert_url         =

    context_type       = ssh
    context_user_id    = peer_gynt
    context_user_pass  =
    context_user_proxy =
    context_user_cert  =
