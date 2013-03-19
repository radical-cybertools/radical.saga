
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


Discussion and Explanation
--------------------------



