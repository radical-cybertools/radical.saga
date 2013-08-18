
Part 3: Remote Job Submission
*****************************

Next, we take the previous example and modify it, so that our job is executed on
a remote machine instead of localhost. This examples shows one of the
most important capabilities of SAGA: abstracting system heterogeneity. We can
use the same code we have used to run a job via 'fork' with minimal
modifications to run a job on a different resource, e.g., via 'ssh' on another
remote system or via 'pbs' or 'sge' on a remote cluster.

Prerequisites 
=============

This example assumes that you have SSH access to a remote resource, either a single host or an HPC cluster.

The example also assumes that you have a working public/private SSH key-pair and
that you can log-in to your remote resource of choice using those keys, i.e.,
your public key is in the ~/.ssh/authorized_hosts file on the remote machine. If
you are not sure how this works, you might want to read about 
`SSH and GSISSH <https://github.com/saga-project/saga-python/wiki/SSH-and-GSISSH>`_ 
first.


Hands-On: Remote Job Submission
===============================

Copy the code from the previous example to a new file saga_example_remote.py.
Add a ``saga.Context`` and ``saga.Session`` right before the ``job.Service`` 
object initialization. Sessions and Contexts describe your SSH identity on the 
remote machine:

.. code-block:: python

    ctx = saga.Context("ssh")
    ctx.user_id = "oweidner" 

    session = saga.Session()
    session.add_context(ctx)

To change the execution host for the job, change the URL in the ``job.Service``
constructor. If you want to use a remote SSH host, use an ssh:// URL. Note that
the session is passed as an additional parameter to the Service constructor:

.. code-block:: python

    js = saga.job.Service("ssh://remote.host.net", session=session)

Alternatively, if you have access to a PBS cluster, use a ``pbs+ssh://...`` URL:

.. code-block:: python
  
    js = saga.job.Service("pbs+ssh://remote.hpchost.net", session=ses)

There are more URL options. Have a look at the :ref:`chapter_adaptors` section
for a complete list. If you submitting your job to a PBS cluster (pbs+ssh://), 
you will probably also have to make some modifications to your ``job.Description``. 
Depending on the configuration of your cluster, you might have to put in the 
name of the queue you want to use or the allocation or project name that should 
be credited:

.. code-block:: python

    jd = saga.job.Description()

    jd.environment     = {'MYOUTPUT':'"Hello from SAGA"'}       
    jd.executable      = '/bin/echo'
    jd.arguments       = ['$MYOUTPUT']
    jd.output          = "mysagajob.stdout"
    jd.error           = "mysagajob.stderr"

    jd.queue           = "short" # Using a specific queue 
    jd.project         = "TG-XYZABCX" # Example for an XSEDE/TeraGrid allocation


Run the Code
------------

Save the file and execute it (**make sure your virtualenv is activated**):

.. code-block:: bash

    python saga_example_remote.py

The output should look something like this:

.. code-block:: none

    Job ID    : None
    Job State : New

    ...starting job...

    Job ID    : [ssh://gw68.quarry.iu.teragrid.org]-[18533]
    Job State : Done

    ...waiting for job...

    Job State : Done
    Exitcode  : 0


Values marked as 'None' could not be fetched from the backend, at that point.


Check the Output
----------------

As opposed to the previous "local" example, you won't find a ``mysagajob.stdout``
file in your working directory. This is because the file has been created on the
remote host were your job was executed. In order to check the content, you would
have to log-in to the remote machine. We will address this issue in the next
example.


Discussion
==========

Besides changing the ``job.Service`` URL to trigger a different middleware
plug-in, we have introduced another new aspect in this tutorial example:
Contexts. Contexts are used to define security / log-in contexts for SAGA
objects and are passed to the executing plug-in (e.g., the SSH plug-in).

A context always has a type that matches the executing plug-in. The two most
commonly used contexts in SAGA are ``ssh`` and ``gsissh``:

.. code-block:: python

    # Your ssh identity on the remote machine
    ctx = saga.Context("ssh")
    ctx.user_id = "oweidner" 

A Context can't be used by itself, but rather has to be added to a
``saga.Session`` object. A session can have one or more Contexts. At runtime,
SAGA Python will iterate over all Contexts of a Session to see if any of them
can be used to establish a connection.

.. code-block:: python

    session = saga.Session()
    session.add_context(ctx)

Finally, Sessions are passed as an extra parameter during object creation, 
otherwise they won't get considered:

.. code-block:: python

    js = saga.job.Service("ssh://remote.host.net", session=ses)

The complete API documentation for Session and Context classes can be found 
in the Library Reference section of this manual. 
