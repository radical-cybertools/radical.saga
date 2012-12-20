Job Management
**************

SAGA's job management module is central to the API. It
represents an application/executable running under the management of a resource 
manager. A resrouce manager can be anything from the local machine to a remote
HPC queing system to grid and cloud computing services.

The basic usage of the job module is as follows::

  # A job.Description object describes the executable/application and its requirements 
  job_desc = saga.job.Description()
  job_desc.executable  = '/bin/sleep'
  job_desc.arguments   = ['10']
  job_desc.output      = 'myjob.out'
  job_desc.error       = 'myjob.err'

  # A job.Service object represents the resource manager. In this example we use the 'local' adaptor to represent the local machine
  service = saga.job.Service('local://localhost')

  # A job is created on a service (resource manager) using the job description
  job = service.create_job(job_desc)
  
  # Run the job and wait for it to finish
  job.run()
  print "Job ID    : %s" % (job.job_id)
  job.wait()

  # Get some info about the job
  print "Job State : %s" % (job.state)
  print "Exitcode  : %s" % (job.exit_code)

Like all SAGA modules, the job module relies on  middleware adaptors 
to provide bindings to a specific resource manager. Adaptors are implicitly 
selected via the `scheme` part of the URL, e.g., ``local://`` in the example 
above selects the `local` job adaptor. The :ref:`job_service` section explains 
this in more detail.

.. note:: A list of available adaptors and supported resource managers can be 
          found in the :ref:`middleware_adaptors` part of this documentation.

The rest of this section is structured as follows:

.. contents:: Table of Contents
   :local:

.. #############################################################################
.. _job_service:

Job Service -- :class:`saga.job.Service`
----------------------------------------

:todo: Describe how to work with services.

.. autoclass:: saga.job.Service
   :members:
   :undoc-members:

Job Description -- :class:`saga.job.Description`
------------------------------------------------

:todo: Describe how to work with description.

.. warning:: There is no guarantee that all middleware adaptors implement 
             all job description attributes. In case a specific attribute
             is not supported, the :meth:`~saga.job.Service.create_job` will throw an 
             exception. Please refer to the :ref:`middleware_adaptors` 
             documentation for more details and adaptor-specific lists of 
             supported attributes.

.. autoclass:: saga.job.Description
   :members:
   :undoc-members:
   :show-inheritance:

SAGA defines the following constants as valid job description attributes:

.. currentmodule:: saga.job
.. autodata:: EXECUTABLE
.. autodata:: ARGUMENTS
.. autodata:: ENVIRONMENT
.. autodata:: WORKING_DIRECTORY
.. autodata:: FILE_TRANSFER
.. autodata:: INPUT
.. autodata:: OUTPUT
.. autodata:: ERROR
.. autodata:: QUEUE
.. autodata:: PROJECT
.. autodata:: SPMD_VARIATION
.. autodata:: TOTAL_CPU_COUNT
.. autodata:: NUMBER_OF_PROCESSES
.. autodata:: PROCESSES_PER_HOST
.. autodata:: THREADS_PER_PROCESS
.. autodata:: INTERACTIVE
.. autodata:: CLEANUP
.. autodata:: JOB_START_TIME
.. autodata:: TOTAL_CPU_TIME
.. autodata:: TOTAL_PHYSICAL_MEMORY
.. autodata:: CPU_ARCHITECTURE
.. autodata:: OPERATING_SYSTEM_TYPE
.. autodata:: CANDIDATE_HOSTS
.. autodata:: JOB_CONTACT

.. _job_job:

Jobs -- :class:`saga.job.Job`
-----------------------------

:todo: Describe how to work with jobs.

.. autoclass:: saga.job.Job
   :members:
   :undoc-members:
   :show-inheritance:

.. _job_attributes:

Attributes 
^^^^^^^^^^

:todo: Explain how to use job attributes

.. currentmodule:: saga.job
.. autodata:: JOB_ID
.. autodata:: EXECUTION_HOSTS
.. autodata:: CREATED
.. autodata:: STARTED
.. autodata:: FINISHED
.. autodata:: EXIT_CODE

.. _job_states:

States
^^^^^^

The job state constants defined describe the possible
states a job can be in. The constants can be used to check / compare the state 
of a job. For example::

  if job.state == saga.job.Pending:
      # do_something
  elif job.state == saga.job.Running:
      # do_something else

The constants also define the string representation of a state::

  >>> str(j.state)
  'Running'

SAGA defines the following constants as job states:

.. currentmodule:: saga.job
.. autodata:: UNKNOWN
.. autodata:: NEW
.. autodata:: PENDING
.. autodata:: RUNNING
.. autodata:: DONE
.. autodata:: CANCELED
.. autodata:: FAILED
.. autodata:: SUSPENDED


.. _job_metrics:

Metrics
^^^^^^^

Job metrics provide a way to attach callback functions to a job object. The
registered callback functions are triggered whenever a job metric changes.

Callback functions require three parameters: 

   :source: the watched object instance
   :metric: the watched metric (e.g. :mod:`STATE` or :mod:`STATE_DETAIL`)
   :value: the new value of the watched metric

Callback functions are attached to a job object via the 
:meth:`~saga.attributes.Attributes.add_callback` method. For example::

  # create a callback function
  def state_cb (self, source, metric, value) :
    print "Job %s state changed to %s : %s"  % (source, value)

  def main () :
    # register the callback function with the 'State' metric
    job.add_callback (saga.job.STATE, state_cb)


.. warning:: There is no guarantee that all middleware adaptors implement 
             these metrics. In case they are not implemented, you can still 
             subscribe to them, but you won't receive any callbacks. Please
             refer to the :ref:`middleware_adaptors` documentation 
             for more details and adaptor-specific lists of supported metrics.


SAGA defines the following constants as job metrics:

.. currentmodule:: saga.job
.. autodata:: STATE
.. autodata:: STATE_DETAIL

.. #############################################################################
.. _job_container:

Job Containers -- :class:`saga.job.Container`
---------------------------------------------

:todo: Describe how to work with job containers.

.. autoclass:: saga.job.Container
   :members:
   :undoc-members:
   :show-inheritance:




.. _examples:

Examples
--------

:todo: example scripts with download (link to example dir)
