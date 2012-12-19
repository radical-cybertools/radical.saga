Job Management
**************

.. todo:: Intro to SAGA job package, etc., including a comprehensive example.

Job Service -- :class:`saga.job.Service`
----------------------------------------

:todo: Describe how to work with services.

.. autoclass:: saga.job.Service
   :members:
   :undoc-members:
   :show-inheritance:


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
.. autodata:: INPUT
.. autodata:: OUTPUT
.. autodata:: ERROR
.. autodata:: FILE_TRANSFER
.. autodata:: CLEANUP
.. autodata:: JOB_START_TIME
.. autodata:: TOTAL_CPU_TIME
.. autodata:: TOTAL_PHYSICAL_MEMORY
.. autodata:: CPU_ARCHITECTURE
.. autodata:: OPERATING_SYSTEM_TYPE
.. autodata:: CANDIDATE_HOSTS
.. autodata:: JOB_CONTACT


Jobs -- :class:`saga.job.Job`
-----------------------------

:todo: Describe how to work with jobs.

.. autoclass:: saga.job.Job
   :members:
   :undoc-members:
   :show-inheritance:

.. _job_attributes:

Job Attributes 
^^^^^^^^^^^^^^

:todo: Explain how to use job attributes

.. currentmodule:: saga.job
.. autodata:: JOB_ID
.. autodata:: EXECUTION_HOSTS
.. autodata:: CREATED
.. autodata:: STARTED
.. autodata:: FINISHED
.. autodata:: EXIT_CODE

.. _job_states:

Job States
^^^^^^^^^^

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

Job Metrics
-----------

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
