
Part 2: Local Job Submission
****************************

One of the most important feature of SAGA Python is the capability to submit
jobs to local and remote queueing systems and resource managers. This first
example explains how to define a SAGA job using the Job API and run it on your
local machine.

If you are somewhat familiar with Python and the principles of distributed
computing, the Hands-On code example is probably all you want to know. The code
is relatively simple and pretty self-explanatory. If you have questions about
the code or if you want to know in detail what's going on, read the Details and
Discussion section further below.

Hands-On: Local Job Submission
==============================

Before we discuss the individual API call in more detail, let's get down and dirty and run our first example: creating and running a SAGA job on your local machine.

Create a new file ``saga_example_local.py`` and paste the following code:

.. literalinclude:: ../../../examples/tutorial/saga_example_local.py


Run the Code
------------

Save the file and execute it (**make sure your virtualenv is activated**):

.. code-block:: bash

    python saga_example_local.py

The output should look something like this:

.. code-block:: none

    Job ID    : [fork://localhost]-[None]
    Job State : saga.job.Job.New

    ...starting job...

    Job ID    : [fork://localhost]-[644240]
    Job State : saga.job.Job.Pending

    ...waiting for job...

    Job State : saga.job.Job.Done
    Exitcode  : None


Check the Output
----------------

Once the job has completed, you will find a file mysagajob.stdout in your current working directory. It should contain the line:

.. code-block:: none

    Hello from SAGA


A Quick Note on Logging and Debugging
-------------------------------------

Since working with distributed systems is inherently complex and much of the
complexity is hidden within SAGA Python, it is necessary to do a lot of internal
logging. By default, logging output is disabled, but if something goes wrong or
if you're just curious, you can enable the logging output by setting the
environment variable ``SAGA_VERBOSE`` to a value between 1 (print only critical
messages) and 5 (print all messages). Give it a try with the above example:

.. code-block:: bash

  SAGA_VERBOSE=5 python saga_example_local.py


Discussion
==========

Now that we have successfully run our first job with saga-python, we will
discuss some of the the building blocks and details of the code.

The job submission and management capabilities of saga-python are packaged in
the `saga.job module (API Doc). Three classes are defined in this module:

* The ``job.Service`` class provides a handle to the resource manager, like for example a remote PBS cluster.
* The ``job.Description`` class is used to describe the executable, arguments, environment and requirements (e.g., number of cores, etc) of a new job.
* The ``job.Job`` class is a handle to a job associated with a job.Service. It is used to control (start, stop) the job and query its status (e.g., Running, Finished, etc).

In order to use the SAGA Job API, we first need to import the saga-python
module:

.. code-block:: python

    import saga

Next, we create a ``job.Service`` object that represents the compute resource you
want to use (see figure above). The job service takes a single URL as parameter.
The URL is a way to tell saga-python what type of resource or middleware you
want to use and where it is. The URL parameter is passed to saga-python's plug-
in selector and based on the URL scheme, a plug-in is selected. In this case the
Local job plug-in is selected for ``fork://``. URL scheme - Plug-in mapping is
described in :ref:`chapter_adaptors`.

.. code-block:: python

    js = saga.job.Service("fork://localhost")

To define a new job, a job.Description object needs to be created that contains
information about the executable we want to run, its arguments, the environment
that needs to be set and some other optional job requirements:

.. code-block:: python

    jd = saga.job.Description()
    
    # environment, executable & arguments
    jd.environment = {'MYOUTPUT':'"Hello from SAGA"'}       
    jd.executable  = '/bin/echo'
    jd.arguments   = ['$MYOUTPUT']

    # output options
    jd.output = "mysagajob.stdout"
    jd.error  = "mysagajob.stderr"
    
Once the ``job.Service`` has been created and the job has been defined via the
``job.Description`` object, we can create a new instance of the job via the
``create_job`` method of the ``job.Service`` and use the resulting object to
control (start, stop) and monitor the job:

.. code-block:: python

    myjob = js.create_job(jd) # create a new job instance
    myjob.run() # start the job instance
    print "Initial Job ID    : %s" % (myjob.jobid)
    print "Initial Job State : %s" % (myjob.get_state())
    myjob.wait() # Wait for the job to reach either 'Done' or 'Failed' state
    print "Final Job ID    : %s" % (myjob.jobid)
    print "Final Job State : %s" % (myjob.get_state())


