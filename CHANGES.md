

* For a list of bug fixes, see
  https://github.com/radical-cybertools/saga-python/issues?q=is%3Aissue+is%3Aclosed+sort%3Aupdated-desc
* For a list of open issues and known problems, see
  https://github.com/radical-cybertools/saga-python/issues?q=is%3Aissue+is%3Aopen+


Version 0.25 release 2014-12-17
---------------------------------------------------------------------

* hotfix for sftp problems on some client/server version combinations which lead
to data inconsistencies


Version 0.24 release 2014-12-08
---------------------------------------------------------------------

* make ssh share mode configurable
* Re-enable explicit_exec and add more explanation.
* Make setting job_type conditional on >1 cores.
* more variety in PBS "constants" (ha!)
* fix #401
* make sure the target dir for leased shells exists on CREATE_PARENTS in dir ctor
* fix #400
* export PPN information to torque and pbs jobs
* merge and fix Danila's patch
* re-enable test for PBSPro_10
* added test config for archer
* re-enable test for PBSPro_10, as discussed with Ole.
* added test config for archer
* add missing error check on mkdir
* fix logical error on dir state recovery
* LoadLeveler support for BG/Q machines.


Version 0.22 release 2014-11-03
---------------------------------------------------------------------

* Hotfix release fixing incompatbile sftp flag "-2"
  See: https://github.com/radical-cybertools/saga-python/issues/397


Version 0.21 release 2014-10-29
---------------------------------------------------------------------

* scattered bug fixes related to connection caching
* configurable switch between scp and sftp
* tweak timeouts on ssh channels
* disable irods adaptor


Version 0.19 release 2014-09-15
---------------------------------------------------------------------

* LeaseManager for connection sharing.
* Improved file transfer performance
* Small improvements in PBS (esp. Cray) and LSF adaptors. 
* Closed tickets:
  - https://github.com/radical-cybertools/saga-python/issues?q=is%3Aclosed+is%3Aissue+milestone%3A%22saga-python+0.19%22+

Version 0.18 release 2014-08-28
---------------------------------------------------------------------

* scattered fixes and perf improvement

Version 0.17 release 2014-07-22
---------------------------------------------------------------------

* Improved prompt-detection and small bug fixes.
* Closed tickets:
  - https://github.com/radical-cybertools/saga-python/issues?milestone=20&state=closed

Version 0.16 release 2014-07-09
---------------------------------------------------------------------

* Several adaptor upddtes
* Addressed SSH caching and prompt-detection issues
* Closed tickets:
  - https://github.com/radical-cybertools/saga-python/issues?milestone=19&state=closed

Version 0.15 release 2014-06-18
---------------------------------------------------------------------

* Fixed issues with the shell aadaptor Directory.list() method:
  - https://github.com/radical-cybertools/saga-python/issues/330

Version 0.14 release 2014-05-07
---------------------------------------------------------------------

* Fixed TTY wrapper issues
* Migration to new GitHub repository
* Documentation now on ReadTheDocs: http://saga-python.readthedocs.org/en/latest/
* Integrated Mark's work on the Condor adaptor
* Closed tickets:
  - https://github.com/radical-cybertools/saga-python/issues?milestone=18&state=closed

Version 0.13 release 2014-02-27
---------------------------------------------------------------------

* Bugfix release.
* Closed tickets:
  - https://github.com/saga-project/saga-python/issues?milestone=17&state=closed


Version 0.12 release 2014-02-26
---------------------------------------------------------------------

* Bugfix release + shell cleanup

Version 0.11 release 2014-02-25
---------------------------------------------------------------------

* Closed tickets:
  - https://github.com/saga-project/saga-python/issues?milestone=16&state=closed

Version 0.10 release 2014-01-18
---------------------------------------------------------------------

* Changed versioning scheme from major.minor.patch to major.minor
  due to Python's messed up installers
* Fixed job script cleanup:
  https://github.com/saga-project/saga-python/issues?milestone=15&state=closed

Version 0.9.16 release 2014-01-13
---------------------------------------------------------------------

* Some improvements to sftp file adaptor
* Closed tickets: 
  - https://github.com/saga-project/saga-python/issues?milestone=14&state=closed

Version 0.9.15 release 2013-12-10
---------------------------------------------------------------------

* Emergency release to fix missing VERSION file

Version 0.9.14 release 2013-12-10
---------------------------------------------------------------------

* Migration to radical.utils
* Numerous SFTP file adaptor improvements
* Closed tickets: 
  - https://github.com/saga-project/saga-python/issues?milestone=13&state=closed

Version 0.9.13 release 2013-11-26
---------------------------------------------------------------------

* Added Platform LSF adaptor
* Closed tickets:
  - https://github.com/saga-project/saga-python/issues?milestone=11&state=closed

Version 0.9.12 release 2013-10-18
---------------------------------------------------------------------

* Added iRODS replica adaptor
* Closed tickets:
  - https://github.com/saga-project/saga-python/issues?milestone=10&state=closed

Version 0.9.11 released 2013-09-04
----------------------------------------------------------------------

* Bugfix release
* Closed tickets:
  - https://github.com/saga-project/saga-python/issues?milestone=9&state=closed

Version 0.9.10 released 2013-08-12
----------------------------------------------------------------------

* Better support for Amazon EC2
* Fixed working directory handling for PBS
* Closed tickets: 
  - https://github.com/saga-project/saga-python/issues?milestone=3&state=closed

Version 0.9.9 released 2013-07-19
----------------------------------------------------------------------

* Hotfix release: bug in Url.__str__ and SFTP copy
  - https://github.com/saga-project/saga-python/issues?milestone=8&state=closed

Version 0.9.8 released 2013-06-22
----------------------------------------------------------------------

* Hotfix release: critical bug in wait() signature
  - https://github.com/saga-project/saga-python/issues?milestone=7&state=closed

Version 0.9.7 released 2013-06-19
----------------------------------------------------------------------

* Added resource package
* Added 'liblcoud' based adaptor to access Amazon EC2 clouds
* Closed issues:
  - https://github.com/saga-project/saga-python/issues?milestone=5&state=closed

Version 0.9.6 released 2013-06-17
----------------------------------------------------------------------

* Hotfix release: critical PBS/TORQUE adaptor fixes
  - https://github.com/saga-project/saga-python/issues?&milestone=6&state=closed

Version 0.9.5 released 2013-06-06
----------------------------------------------------------------------

* Hotfix release: critical SLURM adaptor fixes
  - https://github.com/saga-project/saga-python/issues?milestone=4&state=closed

Version 0.9.4 released 2013-06-01
----------------------------------------------------------------------

* jd.working_directory now gets created if it doesn't exist
* Support for older Cray systems running PBS Pro 10
* Job state callback support for the PBS adaptor - others to follow 
* A simple HTTP protocol file adaptor
* Fixed some issues with user-pass and X.509 security contexts
* Over 40 bugfixes and improvements: 
  - https://github.com/saga-project/saga-python/issues?milestone=2&state=closed

Version 0.9.3 released 2013-04-08
----------------------------------------------------------------------

* Added SFTP adaptor
* Added tutorial examples
* Closed issues:
  - https://github.com/saga-project/saga-python/issues/78
  - https://github.com/saga-project/saga-python/issues/73
  - https://github.com/saga-project/saga-python/issues/72
  - https://github.com/saga-project/saga-python/issues/71
  - https://github.com/saga-project/saga-python/issues/69
  - https://github.com/saga-project/saga-python/issues/66
  - https://github.com/saga-project/saga-python/issues/63
  - https://github.com/saga-project/saga-python/issues/62
  - https://github.com/saga-project/saga-python/issues/61
  - https://github.com/saga-project/saga-python/issues/60
  - https://github.com/saga-project/saga-python/issues/58
  - https://github.com/saga-project/saga-python/issues/57
  - https://github.com/saga-project/saga-python/issues/56
  - https://github.com/saga-project/saga-python/issues/55
  - https://github.com/saga-project/saga-python/issues/22
  - https://github.com/saga-project/saga-python/issues/51
  - https://github.com/saga-project/saga-python/issues/53
  - https://github.com/saga-project/saga-python/issues/26
  - https://github.com/saga-project/saga-python/issues/49
  - https://github.com/saga-project/saga-python/issues/50
  - https://github.com/saga-project/saga-python/issues/47
  - https://github.com/saga-project/saga-python/issues/45
  - https://github.com/saga-project/saga-python/issues/46
  - https://github.com/saga-project/saga-python/issues/43
  - https://github.com/saga-project/saga-python/issues/27

Version 0.9.2 released 2013-03-11
----------------------------------------------------------------------

* Hotfix release

Version 0.9.1 released 2013-03-03
----------------------------------------------------------------------

* Major re-write of engine and adaptor interface
* Support for asynchronous operations 
* Improved PTYWrapper for ssh/gsissh remote execution
* Added SLURM job adaptor 
* Added Condor job adaptor

Version 0.2.7 released 2012-11-09
----------------------------------------------------------------------

* Fixed errors related to pbs://localhost and sge://localhost
  URLs that were caused by a bug in the command-line wrappers.

Version 0.2.6 released 2012-10-25
----------------------------------------------------------------------

* HOTFIX: credential management for SGE and PBS. both adaptors now 
  iterate over SSH and GSISSH contexts as well as consider usernames
  that are part of the url, e.g., pbs+ssh://ole@lonestar.tacc...

Version 0.2.5 released 2012-10-24
----------------------------------------------------------------------

* Changed documentation to Sphinx
* Removed object_type API. Python buildins can be used instead
* Updates to Filesystem API
* Added JobDescription.name attribute (as defined in DRMAA)
* Introduced stateful SSH connection substrate for PBS, SGE, etc
* Introduced support for GSISSH: pbs+gsissh://, sge+gsissh://
* Re-implementation of a (more Python-esque) attribute interface
* Fixed JobID issues, i.e., job.job_id returns 'None' in case the
  job is not running instead of "[serviceurl]-[None]"
* Introduced dynamic, fault-tolerant plug-in loader. If anything 
  goes wrong during loading of a specific plug-in (i.e., dependencies 
  on 3rd party modules cannot be fulfilled, the plug-in will just get 
  skipped and the remaining ones will still get loaded. Previously, a
  single problem during plug-in loading would take Bliss down.

Version 0.2.4 released 2012-7-10
----------------------------------------------------------------------

* Added unit-tests for SPMDVariation
* Added 'mpirun' support for local job plug-in (via SPMDVariation)
* Added some of the missing methods and flags to filesystem package
* An URL object can now be constructed from another URL object
* Fixed job.cancel()
* Wildcard support for Directory.list()

Version 0.2.3 released 2012-6-26
----------------------------------------------------------------------

* Fixed query support for URL class (issue #61)
* Improved logging. No root logger hijacking anymore (issue #62)
* Fixed job.Description.number_of_processes (issue #63)
* Less chatty SSH plug-in (issue #51)

Version 0.2.2 released 2012-6-12
----------------------------------------------------------------------

* job.Decription now accepts strings for int values. This has been
  implemented for backwards compatibility
* Fixed resource.Compute.wait() timeout issue
* Removed excessive SGE/PBS plug-in logging
* job.Service can now be created from a resource.Manager
* Implemented deep copy for description objects
* Runtime now supports multiple plug-ins for the same schema

Version 0.2.1 released 2012-5-16
----------------------------------------------------------------------

* Fixed https://github.com/saga-project/bliss/issues/5
* Fixed https://github.com/saga-project/bliss/issues/13

Version 0.2.0 released 2012-5-15
----------------------------------------------------------------------

* SFTP support for local <-> remote copy operations, mkdir, get_size
* Added supoprt for ssh re-connection after timeout (issue #29)
* Abandoned 'Exception' filenames and API inheritance. The Bliss interface
  looks much cleaner now. Compatibility with previous versions has
  been ensured
* Improved (inline) API documentation
* Swapped urlparse with furl in saga.Url class This hopefully fixes
  the problem with inconsistent parsing accross different Python versions
* Added SGE (Sun Grid Engine) plug-in (issue #11)
* Removed sagacompat compatibility API
* Log source names now all start with 'bliss.'. This should make 
  filtering much easier
* Moved SD package into development branch features/servicediscovery

Version 0.1.19 released 2012-02-29
----------------------------------------------------------------------

* Hotfix - removed experimental Resource plug-in from release

Version 0.1.18 released 2012-02-29
----------------------------------------------------------------------

* Fixed issue with plugin introspection 
* Added template for job plug-in

Version 0.1.17 released 2012-01-04
----------------------------------------------------------------------

* Hotfix

Version 0.1.16 released 2012-01-03
----------------------------------------------------------------------

* Fixed issue: https://github.com/oweidner/bliss/issues/9

Version 0.1.15 released 2012-01-03
----------------------------------------------------------------------

* Fixed issue: https://github.com/oweidner/bliss/issues/8
* Fixed issue: https://github.com/oweidner/bliss/issues/6
* First version of a bigjob plugin. See wiki for details.
* Fixed Python 2.4 compatibility issue

Version 0.1.14 released 2011-12-08
----------------------------------------------------------------------

* Added bliss.sagacompat module for API compatibility.
  API documentation: http://oweidner.github.com/bliss/apidoc-compat/
* Added examples for 'compat' API, e.g.:
  https://github.com/oweidner/bliss/tree/master/examples/job-api/compat/
* Added configuration files for epydoc

Version 0.1.13 released 2011-12-07
----------------------------------------------------------------------

* Fixed executable & argument handling for the local job plugin
* Added support for jd.output and jd.error to local job plugin

Version 0.1.12 released 2011-12-06
----------------------------------------------------------------------

* Fixed bug in URL.get_host()
* Fixed issues with extremely short running PBS jobs 
  in conjunction with scheduler configruations that 
  remove the job from the queue the second it finishes execution.
* First working version of an SFTP file API plugini based on Paramiko
* Two advance bfast examples incl. output file staging:
  https://github.com/oweidner/bliss/blob/master/examples/advanced/bfast_workflow_01.py
  https://github.com/oweidner/bliss/blob/master/examples/advanced/bfast_workflow_02.py

Version 0.1.11 released 2011-11-28
----------------------------------------------------------------------

* Fixed issues with PBS working directory 
* Added simple job API example that uses BFAST:
  https://github.com/oweidner/bliss/blob/master/examples/job-api/pbs_via_ssh_bfast_job.py
* Updated apidoc: http://oweidner.github.com/bliss/apidoc/
* First prototype of a job container. Example can be found here:
  https://github.com/oweidner/bliss/blob/master/examples/job-api/pbs_via_ssh_container.py  
* Implemented CPU and Memory information via PBS service discovery
* Changed job.Description.walltime_limit to 
  job.Description.wall_time_limit

Version 0.1.10 released 2011-11-16
----------------------------------------------------------------------

* Fixed issue with local job plugin

Version 0.1.9 released 2011-11-16
----------------------------------------------------------------------

* Prototype of a Service Discovery packages
* PBS/SSH support for service discovery

Version 0.1.8 released 2011-11-09
----------------------------------------------------------------------

* Fixed issue with PBS plugin job.wait()

Version 0.1.7 released 2011-11-09
----------------------------------------------------------------------

* More or less stable job API    
* First functional PBS over SSH plugin 

