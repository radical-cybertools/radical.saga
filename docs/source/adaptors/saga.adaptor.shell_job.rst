
********************
saga.adaptor.ssh_job
********************

 
A more elaborate description....
Known Limitations:
------------------
* number of system pty's are limited:  each job.service object bound
to this adaptor will use 2 pairs of pty pipes.  Systems usually
limit the number of available pty's to 1024 .. 4096.  Given that
other process also use pty's , that gives a hard limit to the number
of object instances which can be created concurrently.  Hitting the
pty limit will cause the following error message (or similar)::
NoSuccess: pty_allocation or process creation failed (ENOENT: no more ptys)
This limitation comes from saga.utils.pty_process.  On Linux
systems, the utilization of pty's can be monitored::
echo "allocated pty's: `cat /proc/sys/kernel/pty/nr`"
echo "available pty's: `cat /proc/sys/kernel/pty/max`"
* number of ssh connections are limited: sshd's default configuration,
which is in place on many systems, limits the number of concurrent
ssh connections to 10 per user -- beyond that, connections are
refused with the following error::
NoSuccess: ssh_exchange_identification: Connection closed by remote host
As the communication with the ssh channel is unbuffered, the
dropping of the connection will likely cause this error message to
be lost.  Instead, the adaptor will just see that the ssh connection
disappeared, and will issue an error message similar to this one::
NoSuccess: read from pty process failed (Could not read line - pty process died)
* number of processes are limited: the creation of an job.service
object will create one additional process on the local system, and
two processes on the remote system (ssh daemon clone and a shell
instance).  Each remote job will create three additional processes:
two for the job instance itself (double fork), and an additional
process which monitors the job for state changes etc.  Additional
temporary processes may be needed as well.  
While marked as 'obsolete' by POSIX, the `ulimit` command is
available on many systems, and reports the number of processes
available per user (`ulimit -u`)
On hitting process limits, the job creation will fail with an error
similar to either of these::
NoSuccess: failed to run job (/bin/sh: fork: retry: Resource temporarily unavailable)
NoSuccess: failed to run job -- backend error
* number of files are limited, as is disk space: the job.service will
keep job state on the remote disk, in ``$HOME/.saga/adaptors/ssh_job/``.
Quota limitations may limit the number of files created there,
and/or the total size of that directory.  
On quota or disk space limits, you may see error messages similar to
the following ones::
NoSuccess: read from pty process failed ([Errno 5] Quota exceeded)
NoSuccess: read from pty process failed ([Errno 5] Input/output error)
NoSuccess: find from pty process [Thread-5] failed (Could not read - pty process died)
* Other system limits (memory, CPU, selinux, accounting etc.) apply as
usual.
* thread safety: it is safe to create multiple :class:`job.Service`
instances to the same target host at a time -- they should not
interfere with each other, but ``list()`` will list jobs created by
either instance (if those use the same target host user account).
It is **not** safe to use the *same* :class:`job.Service` instance
from multiple threads concurrently -- the communication on the I/O
channel will likely get screwed up.  This limitation may be removed
in future versions of the adaptor.  Non-concurrent (i.e. serialized)
use should work as expected though.
* the adaptor option ``enable_debug_trace`` will create a detailed
trace of the remote shell execution, on the remote host.  This will
interfere with the shell's stdio though, and may cause unexpected
failures.  Debugging should only be enabled as last resort, e.g.
when logging on DEBUG level remains inconclusive, and should
**never** be used in production mode.


Version
=======

v0.1


Supported Schemas
=================

  - **fork** : use /bin/sh to run jobs
  - **gsissh** : use gsissh to run remote jobs
  - **ssh** : use ssh to run remote jobs



Configuration Options
=====================

``enabled``

enable / disable saga.adaptor.ssh_job adaptor

  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]
``enable_debug_trace``

Create a detailed debug trace on the remote host.
Note that the log is *not* removed, and can be large!
A log message on INFO level will be issued which
provides the location of the log file.

  - **type** : <type 'bool'>
  - **default** : False
  - **environment** : None
  - **valid options** : [True, False]
``enable_notifications``

Enable support for job state notifications.  Note that
enabling this option will create a local thread, a remote 
shell process, and an additional network connection.
In particular for ssh/gsissh where the number of
concurrent connections is limited to 10, this
effectively halfs the number of available job service
instances per remote host.

  - **type** : <type 'bool'>
  - **default** : False
  - **environment** : None
  - **valid options** : [True, False]


Supported Capabilities
======================

``Supported Monitorable Metrics``

  - State
  - StateDetail

``Supported Job Attributes``

  - ExitCode
  - ExecutionHosts
  - Created
  - Started
  - Finished

``Supported Context Types``

  - *x509*: X509 proxy for gsissh
  - *userpass*: username/password pair for simple ssh
  - *ssh*: public/private keypair

``Supported Job Description Attributes``

  - Executable
  - Arguments
  - Environment
  - Input
  - Output
  - Error



Supported API Classes
=====================

  - :class:`saga.job.Service`
  - :class:`saga.job.Job`


saga.job.Service
""""""""""""""""

.. autoclass:: saga.adaptors.ssh.ssh_job.SSHJobService
   :members:


saga.job.Job
""""""""""""

.. autoclass:: saga.adaptors.ssh.ssh_job.SSHJob
   :members:



