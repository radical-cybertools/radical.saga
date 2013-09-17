.. _job_managemen:

Job Submission and Control
===========================

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

   service.close()

.. seealso:: More examples can be found in the individual adaptor sections!

Like all SAGA modules, the job module relies on  middleware adaptors 
to provide bindings to a specific resource manager. Adaptors are implicitly 
selected via the `scheme` part of the URL, e.g., ``local://`` in the example 
above selects the `local` job adaptor. The :ref:`job_service` section explains 
this in more detail.

.. note:: A list of available adaptors and supported resource managers can be 
          found in the :ref:`chapter_middleware_adaptors` part of this 
          documentation.

The rest of this section is structured as follows:

.. contents:: Table of Contents
   :local:

.. #############################################################################
.. _job_service:

Job Service -- :class:`saga.job.Service`
----------------------------------------

.. autoclass:: saga.job.Service
   :members:
   :undoc-members:
   :special-members: __init__
   :exclude-members: create, __module__

Job Description -- :class:`saga.job.Description`
------------------------------------------------

**Warning:** There is no guarantee that all middleware adaptors implement all job
description attributes. In case a specific attribute is not supported, the
:meth:`~saga.job.Service.create_job` will throw an exception. Please refer to
the :ref:`chapter_middleware_adaptors` documentation for more details and
adaptor-specific lists of supported attributes.

.. autoclass:: saga.job.Description
   :members:
   :undoc-members:
   :show-inheritance:

SAGA defines the following constants as valid job description attributes:

.. currentmodule:: saga.job
.. data:: EXECUTABLE

   The executable to start once the job starts running::

       jd = saga.job.Description()
       jd.executable = "/bin/sleep"

   :type: str

.. data:: executable 

   Same as attribute :data:`~saga.job.EXECUTABLE`.

.. data:: ARGUMENTS

   Arguments to pass to the :data:`~saga.job.EXECUTABLE`::

       jd = saga.job.Description()
       jd.arguments = ['--flag1', '--flag2']

   :tpye: list()

.. data:: arguments 

   Same as attribute :data:`~saga.job.ARGUMENTS`.

.. data:: ENVIRONMENT

   Environment variables to set in the job's context::

       jd = saga.job.Description()
       jd.environemnt = {'FOO': 'BAR', 'FREE': 'BEER'}

   :type: dict()

.. data:: environment 

   Same as attribute :data:`~saga.job.ENVIRONMENT`.

.. data:: WORKING_DIRECTORY

   The working directory of the job::

       jd = saga.job.Description()
       jd.working_directory = "/scratch/experiments/123/"

   :type: str()

.. data:: working_directory 

   Same as attribute :data:`~saga.job.WORKING_DIRECTORY`.

.. data:: OUTPUT

   Filename to capture the executable's STDOUT stream. If ``output`` is 
   a relative filename, the file is relative to :data:`~saga.job.WORKING_DIRECTORY`::

       jd = saga.job.Description()
       jd.output = "myjob_stdout.txt"

   :type: str()

.. data:: output 

   Same as attribute :data:`~saga.job.OUTPUT`. 


.. data:: ERROR

   Filename to capture the executable's STDERR stream. If ``error`` is 
   a relative filename, the file is relative to :data:`~saga.job.WORKING_DIRECTORY`::

       jd = saga.job.Description()
       jd.error = "myjob_stderr.txt"

   :type: str()

.. data:: error 

   Same as attribute :data:`~saga.job.ERROR`.

.. data:: FILE_TRANSFER

   Files to stage-in before the job starts running and to stage out once 
   the job has finished running. The syntax is as follows:

      ``local_file OPERATOR remote_file``

   ``OPERATOR`` can be one of the following:

   * ``>`` copies the local file to the remote fille before the job starts. Overwrites the remote file if it exists.
   * ``<`` copies the remote file to the local file after the job finishes. Overwrites the local file if it exists

   Example::

       jd = saga.job.Description()
       jd.input_file_transfer = ["file://localhost/data/input/test.dat > "test.dat",
                                 "file://localhost/data/results/1/result.dat < "result1.dat"
                                ]

   :type: list()

.. data:: file_transfer 

   Same as attribute :data:`~saga.job.FILE_TRANSFER`.

.. data:: QUEUE

   The name of the queue to submit the job to:: 

       jd = saga.job.Description()
       jd.queue = "mpi_long"

   :type: str()

.. data:: queue 

   Same as attribute :data:`~saga.job.QUEUE`.

.. data:: PROJECT

   The name of the project / allocation to charged for the job :: 

       jd = saga.job.Description()
       jd.project = "TG-XYZ123456"

   :type: str()

.. data:: project 

   Same as attribute :data:`~saga.job.PROJECT`.



.. data:: SPMD_VARIATION

   Describe me! 

   :type: str()

.. data:: spmd_variation 

   (Property) Same as attribute :data:`~saga.job.SPMD_VARIATION`.



.. autodata:: TOTAL_CPU_COUNT
.. data:: total_cpu_count 

   (Property) Same as attribute :data:`~saga.job.TOTAL_CPU_COUNT`.

   :type: int() or str()

.. autodata:: NUMBER_OF_PROCESSES
.. data:: number_of_processes 

   (Property) Same as attribute :data:`~saga.job.NUMBER_OF_PROCESSES`.

   :type: int() or str()

.. autodata:: PROCESSES_PER_HOST 
.. data:: processes_per_host 

   (Property) Same as attribute :data:`~saga.job.PROCESSES_PER_HOST`.

   :type: int() or str()

.. autodata:: THREADS_PER_PROCESS
.. data:: threads_per_process 

   (Property) Same as attribute :data:`~saga.job.THREADS_PER_PROCESS`.

   :type: int() or str()

# NOT IMPLEMENTED.. autodata:: INTERACTIVE

.. autodata:: CLEANUP
.. data:: cleanup 

   (Property) Same as attribute :data:`~saga.job.CLEANUP`.

   :type: bool()

.. autodata:: JOB_START_TIME
.. data:: job_start_time 

   (Property) Same as attribute :data:`~saga.job.JOB_START_TIME`.

   :type: UNIX timestamp

.. autodata:: WALL_TIME_LIMIT
.. data:: wall_time_limit 

   (Property) Same as attribute :data:`~saga.job.WALL_TIME_LIMIT`.

.. autodata:: TOTAL_PHYSICAL_MEMORY
.. data:: total_physical_memory 

   (Property) Same as attribute :data:`~saga.job.TOTAL_PHYSICAL_MEMORY`.

.. autodata:: CPU_ARCHITECTURE
.. data:: cpu_architecture 

   (Property) Same as attribute :data:`~saga.job.CPU_ARCHITECTURE`.

.. autodata:: OPERATING_SYSTEM_TYPE
.. data:: operating_system_type 

   (Property) Same as attribute :data:`~saga.job.OPERATIN_SYSTEM_TYPE`.

.. autodata:: CANDIDATE_HOSTS
.. data:: candidate_hosts 

   (Property) Same as attribute :data:`~saga.job.CANDIDATE_HOSTS`.

.. autodata:: JOB_CONTACT
.. data:: job_contact 

   (Property) Same as attribute :data:`~saga.job.JOB_CONTACT`.


.. _job_job:

Jobs -- :class:`saga.job.Job`
-----------------------------

.. autoclass:: saga.job.Job
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: get_result, get_object, get_exception, re_raise ,result, object, exception, get_stdin, get_stdout, get_stderr, suspend, resume, migrate, checkpoint, stdout, stderr, __module__


.. _job_attributes:

Attributes 
^^^^^^^^^^

.. currentmodule:: saga.job
.. autodata:: ID
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


**Warning:** There is no guarantee that all middleware adaptors implement these
metrics. In case they are not implemented, you can still subscribe to them, but
you won't receive any callbacks. Please refer to the
:ref:`chapter_middleware_adaptors` documentation for more details and
adaptor-specific lists of supported metrics.


SAGA defines the following constants as job metrics:

.. currentmodule:: saga.job
.. autodata:: STATE
.. autodata:: STATE_DETAIL

.. #############################################################################
.. _job_container:

Job Containers -- :class:`saga.job.Container`
---------------------------------------------

.. seealso:: More examples on how to use job containers can be found in 
             the :ref:`code_examples_job` section of the 
             :ref:`chapter_code_examples` chapter.

.. autoclass:: saga.job.Container
   :members:
   :undoc-members:
   :show-inheritance:

