

For a list of open issues and known problems, see:
https://github.com/radical-cybertools/radical.saga/issues

Version 1.6.6 Release                                                 2021-05-18
--------------------------------------------------------------------------------

  - linting, GH actions


Version 1.6.5 Release                                                 2021-04-14
--------------------------------------------------------------------------------

  - fix state update notifications on `CANCELED`


Version 1.6.1 Release                                                 2021-03-09
--------------------------------------------------------------------------------

  - bridges2 support


Version 1.5.9 Release                                                 2021-01-22
--------------------------------------------------------------------------------

  - address race condition on cancellation
  - re-enable prompt escape
  - fix #787


Version 1.5.8 Release                                                 2020-12-26
--------------------------------------------------------------------------------

  - Comet takes --gres for gpu
  - extended list of valid alloc_flags options for Lassen
  - ip-isolate flag for distributed pytorch on Lassen
  - updated slurm script record for Comet


Version 1.5.7 Release                                                 2020-10-30
--------------------------------------------------------------------------------

  - added QoS option for special queue "tmp3" at SuperMUC-NG
  - fixed the case when job is routed to a different queue,
    thus SAGA shouldn't fail the job in this case (Cobalt)
  - rtx switch for ppn


Version 1.5.6 Hotfix Release                                          2020-09-29
--------------------------------------------------------------------------------

  - better fix ssh timeout


Version 1.5.5 Hotfix Release                                          2020-09-28
--------------------------------------------------------------------------------

  - fix ssh timeout on idle (but alive) connections


Version 1.5.4 Release                                                 2020-09-14
--------------------------------------------------------------------------------

  - added job description attribute `SystemArchitecture`
  - removed `summitdev` from LSFJob, fix cobalt/theta settings
  - updates for LSF and SLURM job modules


Version 1.5.2 Release                                                 2020-08-05
--------------------------------------------------------------------------------

  - fix a python3 transition remnant
  

Version 1.5.1 Release                                                 2020-08-05
--------------------------------------------------------------------------------

  - Access to K80 and P100 gpus on Bridges
  - added exception if number of nodes is not set, but memory is allocated
  - get `parse_qs` from `urllib.parse` (module `cgi` is not used in SAGA)
  - replace PIL with Pillow
  - set min python version to 3.6

      
Version 1.4.0 Release                                                 2020-05-12
--------------------------------------------------------------------------------

  - merge #782: Lassen support
  - support for Frontera's RTX queue

      
Version 1.3.0 Release                                                 2020-04-10
--------------------------------------------------------------------------------

  - pr/768: Removing some warnings
  - pr/769: fix ppn for frontera
  - pr/771: fixed handling of Longhorn's sbatch
  - pr/776: clean up logs
  - pr/778: traverse support also ensure baclward compatibility for cgi module
  - add example for cobalt
  - doc fixes (issue #765)
  - First commit fixing the docs
  - apply exception chaining and some exception / logging cleanup
  - longhorn ppn fix
  - mira -> theta
  - resolve name conflict with threading module

      
Version 1.2.0 Release                                                 2020-03-07
--------------------------------------------------------------------------------

  - fix ppn for frontera
  - documentation fixing (issue #765)


Version 1.1.2 Hotfix Release                                          2020-02-22
--------------------------------------------------------------------------------

  - fix MANIFEST.in


Version 1.1.1 Hotfix Release                                          2020-02-21
--------------------------------------------------------------------------------

  - ensure '-n' beingused for Stamepede2

      
Version 1.1 Release                                                   2020-02-11
--------------------------------------------------------------------------------

  - python 3 and later have implicit namespaces
  - add noop job adaptor for tests
  - slurm data staging
    

Version 1.0.1  Release                                                2020-01-23
--------------------------------------------------------------------------------

  - small fixes and cleanup for slurm and docs (tutorial prep)


Version 1.0.0  Release                                                2019-12-24
--------------------------------------------------------------------------------

  - transition to Python3
  - fix issue #744
  - config fixes
  - desable logger during gc
  - fix attribute callback
  - fix generated slurm script (walltime format)
  - fix slurm for rhea
  - rename async as it is a reserved word in p3
  - testing, flaking, linting and travis fixes


Version 0.72.1  Hotfix Release                                        2019-09-17
--------------------------------------------------------------------------------

  - tiger's Slurm needs `--chdir` now (thanks Lucas!)


Version 0.72.0  Release                                               2019-09-11
--------------------------------------------------------------------------------

  - improved support GPUs in lsf (tiger)
  - frontera support


Version 0.70.0  Release                                               2019-07-07
--------------------------------------------------------------------------------

  - support GPUs in lsf
  - fix state notifications in lsf
  - default lsf is summit-enabled
  - torque: use checkjob on failing qstat


Version 0.62.0  Release                                               2019-06-08
--------------------------------------------------------------------------------

  - better cray detection
  - code improvements
  - upport srun as (unsccheduled or scheduled) launch method
  - support tiger @ princeton
  - remove support for some legacy machines


Version 0.60.0  Release                                               2019-04-10
--------------------------------------------------------------------------------

  - radicalization! rename saga-python to radical.saga
  - fix logger levels
  - fix issue #661
  - Adding missing flags for Summit. add test case
  - fix job.cancel in some corner cases
  - document missing path normalization
  - add support for summit
  - get hostname from env variable
  - clean out unsupported jod description attributes
  - LSF SMT level now defaults to 1 (summit)
  - convert to new config file format (json)
  - get unit tests back in  working order
  - gpu support for bridges
  - linted, flaked, pepped, cleaned...  a bit...
  - make commtransparent flag optional
  - remove topology restriction on BW
  - sync torque and pbspro, remove deprecated pbs
  - make job adaptors uniformely emit EPOCH timestamps


Version 0.50.5                                                        2018-12-19
--------------------------------------------------------------------------------

  - fixes in Slurm and Torque adaptor


Version 0.50.4                                                        2018-12-12
--------------------------------------------------------------------------------

  - fix version check in slurm adaptor


Version 0.50.3                                                        2018-11-13
--------------------------------------------------------------------------------

  - fix version check in face of git errors


Version 0.50.2                                                        2018-11-13
--------------------------------------------------------------------------------

  - fix version check for Stampede (#rp-1754) - thanks Ioannis!


Version 0.50.1                                                        2018-10-26
--------------------------------------------------------------------------------

  - Add Cheyenne suport - thanks Vivek!


Version 0.50.0                                                        2018-0&-03
--------------------------------------------------------------------------------

  - partial support for heterogeneous clusters (slurm)
  - (origin/pr/637) more thorough handling of $PROMPT_COMMAND
  - Fix #680 - Corrected Error on LSF: module object has no attribute Event
  - Fix/issue 1514
  - Fix/issue 662
  - Fix/issue 663
  - add gpu "support" to torque
  - correctly interprete candidate_hosts on pbspro
  - fix torque job name handling
  - make sure theh default session also inherits the uid (#671)
  - remove obsolete comment, add local GPUs
  - sync pbspro and torque adaptor.


Version 0.47.6                                                        2018-06-02
--------------------------------------------------------------------------------

  - catch up with  RU log, rep and prof settings


Version 0.47.5                                                        2018-04-20
--------------------------------------------------------------------------------

  - slurm uses '-N' on wrangler now.


Version 0.47.4                                                        2018-04-09
--------------------------------------------------------------------------------

  - trigger BW syntax based on version string


Version 0.47.3                                                        2018-03-20
--------------------------------------------------------------------------------

  - accept SID settings from upper layers (RP) (#654)
  - add travis tests, badge
  - cheyenne fix to pbspro
  - get titan back to work (#664)
  - fix task container get_state reval


Version 0.47.2                                                        2018-02-28
--------------------------------------------------------------------------------

  - accept SID settings from upper layers (RP) (#654)
  - fix task container get_state reval


Version 0.47                                                          2017-11-19
--------------------------------------------------------------------------------

  - avoid use of `condor_history` if so configured
  - cleaner recovery for `condor_hist` when its used
  - be more resilient against `sacct` errors
  - one more `PROMPT_COMMAND` setting


Version 0.46                                                          2017-08-23
--------------------------------------------------------------------------------

  - hotfix for RP #1415


Version 0.46                                                          2017-08-11
--------------------------------------------------------------------------------

  - Fix several debug messages
  - Fix/621 experiment aimes (#622)
  - Fixed run_job to run in service.py
  - Properly support SGE job name. (#624)
  - Anselm support in pbspro v.13 (#634)
  - fix a condor script syntax error
  - attempt to prevent job eviction
  - be resiliant against lingering NFS locks (hi titan)
  - clean bulk job info for condor
  - clean up state management in condor adaptor,
  - slim down condor log calls, simplify status updates, ensure output transfer
  - container_cancel needs to accept timeout parameter (#633)
  - don't barf on failing condor_history
  - don't limit status check length
  - iteration on slurm mpi support (#623)
  - fix logic error in directive evaluation
  - fix parsing of condor_history for multi-file staging
  - fixes on src/tgt ordering in osg staging
  - follow the rename of ru.Task to ru.Future
  - follow the rename of ru.Thread to ru.Future
  - implement bulk cancel for condor
  - iteration on bulk submission, data staging
  - iteration on run_job implementatio
  - improve condor scaling / performance
  - make sure we alsways have a valid transfer directive in condor
  - make sure we do not fail on a missing exit code
  - more clarity on file staging semantics, some cleanup
  - better handling of $PROMPT_COMMAND
  - resilience against condor_history errors
  - update on torque to avoid triggering a check


Version 0.45.1                                                        2017-02-28
--------------------------------------------------------------------------------

  - hotfix to support CandidateHosts for LoadLeveler


Version 0.45                                                          2017-02-28
--------------------------------------------------------------------------------

  - Add srm adaptor - Thanks Mark!
  - Add cobalt adaptor (blue gene/q) - Thanks Manuel!
  - Add special case for Rhea
  - Deal with timeouts.
  - Don't want our jobs to restart after eviction (OSG)
  - Make pty shell url configurable.
  - Remove some more PBSPro remains.
  - address #585
  - fix #590
  - check that prev_info exists before populating curr_info with its info in _job_get_info
  - clean up slurm adaptor to get it fit for the split branch in RP
  - fix state interpretation for pbspro
  - make torque fit for rp split branch
  - remove some debug logs
  - some consistency fixes
  - update slurm example in context of #611
  - use `-o Port=%d` notation for ssh based channels
  - backport ft and bulk ops for torque from osg_optimization


Version 0.44                                                          2016-11-01
--------------------------------------------------------------------------------

  - added basework_dir parameter to sge and proxy adaptors
  - added new PBS versions to pbsnodes CPU count check
  - changed the regular expression to find the job-id on LSF adaptor. Fixes #568
  - re-enable fallback methods for slurm job containers, jd.name support for slurm
  - fix for issue #586, removing invalid dirs from shell wrapper script file path
  - fix parsing of file staging directives
  - make shell job adaptor workdir configurable
  - avoid double close for shell job service
  - raise error on missing tools
  - removed duplicate get_name function in job class
  - add missing container method to get job states
  - merged pull request #583
  - enforce version in radical stack
  - fix #555


Version 0.41.3                                                        2016-07-07
--------------------------------------------------------------------------------

  - still hating it...


Version 0.41.2                                                        2016-07-07
--------------------------------------------------------------------------------

  - I hate python deployment


Version 0.41.1                                                        2016-07-07
--------------------------------------------------------------------------------

  - hotfix: remove some debug code which causes trouble in multiuser envs


Version 0.41                                                          2016-06-02
--------------------------------------------------------------------------------

  - Feature/job name shell (#541)
  - implementation of job names for shell job adaptor and on API level
  - add job.name for condor
  - address #552
  - allow for non-interative local pty shells
  - initialidir support for Condor
  - change HOSTFILE settings

  - fix missing var setting in aws example
  - fix port opening directive, make sure port is opened just once
  - fix regression in job cancellation
  - fix rel path on open
  - fix staging calls
  - fix string formatting error
  - fix ssh context to handle passwords containing spaces
  - cleanup of the shell spawner, getting in sync with RP version
  - implement dir.exists
  - make sure we have a job description on reconnected jobs
  - make task passing to async calls optional
  - aws security group tweaking
  - remove invalid obj redirection
  - code simplification
  - simplify local mkdir in shell file adaptor
  - sync with updated benchmark tools
  - use shared shell connection for FS ops


Version 0.40.2                                                        2016-04-23
--------------------------------------------------------------------------------

  - Hotfix release to avoid security warnings on Stampede


Version 0.40.1                                                        2016-02-05
--------------------------------------------------------------------------------

  - Hotfix release to address a tmp file race condition on file staging


Version 0.40                                                          2016-01-19
--------------------------------------------------------------------------------

  - Added job monitor state update fix to PBSPro adapter.
  - Add session property to base class. Fix #480.
  - add traceback property to exception
  - support gsissh for condor job submission
  - pass span parameter to LSF.
  - support SLURM reservation.
  - file staging for shell adaptor
  - Fix #477, set session for shell job service
  - Fixed job state monitor to correctly identify state changes.
  - Fixed string formatting error.
  - Fixes #501. Thanks Javi!
  - fix session documentation.  Thanks Jeremy!
  - Fix to allow use of environment vars in ssh context key/cert property file paths.
  - Implement bulk submit, states and wait for condor.
  - Improve file staging directives handling.
  - Logging goes into working directory.
  - make ssh connection timeout configirable (defaults now to 10 seconds)
  - Passing ssh_timeout param to ssh ConnectTimeout option
  - Updated fix to #494 to take account of empty key/cert parameter.
  - deprecate PBS adaptor


Version 0.39                                                          2015-12-01
--------------------------------------------------------------------------------

  - support dynamic adaptor loading
  - fix #477, set session for shell job service (thanks Mehdi!)
  - set session on file and directory instances, #480


Version 0.38.1 release                                                2015-11-11
--------------------------------------------------------------------------------

  - fix 0.38 after botched merge


Version 0.38 release                                                  2015-11-06
--------------------------------------------------------------------------------

  - support for anaconda client install


Version 0.37 release                                                  2015-10-15
--------------------------------------------------------------------------------

  - scattered bug fixes


Version 0.36 release                                                  2015-10-15
--------------------------------------------------------------------------------

  - update of GO adaptor with recent GlobusOnline evolotion
  - scattered fixes in GO adaptor


Version 0.36 release                                                  2015-10-08
--------------------------------------------------------------------------------

  Note that RADICAL_SAGA_VERBOSE should now be used instead of SAGA_VERBOSE (but
  the latter will be supported for a while).  PTY layer debug levels can
  separately be enabled via RADICAL_SAGA_PTY_VERBOSE.

  - fix cray qsub arguments
  - fix interpretation of relative / absolute URLs in some cases
  - fix #449 - thanks jcohen02!
  - keep up with logger changes in util
  - properly detect failed jobs.
  - follow changes on resource configurations (BW)
  - remove dead code.
  - fix state mapping in some queue adaptors
  - clean torque/pbs separation


Version 0.35 release                                                  2015-07-14
--------------------------------------------------------------------------------

  - Add contexts to session at start of context list
  - add tc.get_task(id)
  - Add PROCESSES_PER_HOST to all job adaptors.
  - sync an sdist naming fix
  - Add dedicated PBS Pro adaptor.
  - Get ppn from proccess_per_host.
  - implement candidate_hosts for slurm adaptor
  - processes_per_host for SLURM.


Version 0.29 release                                                  2015-07-14
--------------------------------------------------------------------------------

  - apply setup/git fix
  - fix exception type exception
  - convert to locking "with"/by context manager.
  - only create parents for the dir part of a target.
  - reentrant lock to guard concurrent cache writing.
  - fix object._id format


Version 0.28 release 2015-04-16
--------------------------------------------------------------------------------

  - set 'ssh_share_mode=no' on CentOS
  - resilience against missing git in setup.py
  - Prompt pattern for RSA SecureID (BW, Titan, etc.)
  - cleaner version string
  - treat empty strings as unset values in PBS job description
  - fix slurm script generation
  - implement discussed changes to job stdio inspection
  - finish implementation of #202
  - pre- and post-exec for shell job adaptor
  - implement pre/post_exec, support in slurm, cleanup slurm submission
  - clean up stdin/stdout/stderr/log handling for jobs
  - sync setup.py with recent changes in RP
  - Fix rounding up cores for blacklight
  - ignore SIGHUP in the shell monitor -- fixes #415
  - install sdist, export sdist location


Version 0.27 release 2015-03-25
--------------------------------------------------------------------------------

  - fix rounding-up cores for blacklight


Version 0.26 release 2015-02-24
--------------------------------------------------------------------------------

  - Major iteration on Globus Online adaptor, including tests and
    examples
  - move sources into src/
  - short_version -> version
  - long_version -> version_detail
  - use DebugHelper in test suite
  - implement #413
  - Check status of task to be able to detect failure.
  - fix to make async tasks working for methods which do not provide
    metrics
  - Fix ssh key logic.
  - do not pick up pem certs by default
  - Add "gres" (Generic Resource) query parameter.
  - Add note about queue query parameter.
  - fix syntax error in PBS variable spec
  - Fix, cleanup and document qstat parsing.
  - Document job states.
  - stop job monitoring on continous errors (pbs)
  - Support "bigflash" nodes selection on Gordon.
  - make workdir for shell wrapper a parameter
  - force copy shells to be non-posix (sftp)
  - make sure that non-posix shells are excused from prompt triggering


Version 0.25 release 2014-12-17
--------------------------------------------------------------------------------

  - hotfix for sftp problems on some client/server version
    combinations which lead to data inconsistencies


Version 0.24 release 2014-12-08
--------------------------------------------------------------------------------

  - make ssh share mode configurable
  - Re-enable explicit_exec and add more explanation.
  - Make setting job_type conditional on >1 cores.
  - more variety in PBS "constants" (ha!)
  - fix #401
  - make sure the target dir for leased shells exists on
    CREATE_PARENTS in dir ctor
  - fix #400
  - export PPN information to torque and pbs jobs
  - merge and fix Danila's patch
  - re-enable test for PBSPro_10
  - added test config for archer
  - re-enable test for PBSPro_10, as discussed with Ole.
  - added test config for archer
  - add missing error check on mkdir
  - fix logical error on dir state recovery
  - LoadLeveler support for BG/Q machines.


Version 0.22 release 2014-11-03
--------------------------------------------------------------------------------

  - Hotfix release fixing incompatbile sftp flag "-2"
    - https://github.com/radical-cybertools/radical.saga/issues/397


Version 0.21 release 2014-10-29
--------------------------------------------------------------------------------

  - scattered bug fixes related to connection caching
  - configurable switch between scp and sftp
  - tweak timeouts on ssh channels
  - disable irods adaptor


Version 0.19 release 2014-09-15
--------------------------------------------------------------------------------

  - LeaseManager for connection sharing.
  - Improved file transfer performance
  - Small improvements in PBS (esp. Cray) and LSF adaptors.
  - Closed tickets:
    - https://github.com/radical-cybertools/radical.saga/issues?q=is%3Aclosed+is%3Aissue+milestone%3A%22radical.saga+0.19%22+

Version 0.18 release 2014-08-28
--------------------------------------------------------------------------------

  - scattered fixes and perf improvement

Version 0.17 release 2014-07-22
--------------------------------------------------------------------------------

  - Improved prompt-detection and small bug fixes.
  - Closed tickets:
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=20&state=closed

Version 0.16 release 2014-07-09
--------------------------------------------------------------------------------

  - Several adaptor upddtes
  - Addressed SSH caching and prompt-detection issues
  - Closed tickets:
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=19&state=closed

Version 0.15 release 2014-06-18
--------------------------------------------------------------------------------

  - Fixed issues with the shell aadaptor Directory.list() method:
    - https://github.com/radical-cybertools/radical.saga/issues/330

Version 0.14 release 2014-05-07
--------------------------------------------------------------------------------

  - Fixed TTY wrapper issues
  - Migration to new GitHub repository
  - Documentation now on ReadTheDocs:
    http://radical.saga.readthedocs.org/en/latest/
  - Integrated Mark's work on the Condor adaptor
  - Closed tickets:
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=18&state=closed

Version 0.13 release 2014-02-27
--------------------------------------------------------------------------------

  - Bugfix release.
  - Closed tickets:
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=17&state=closed


Version 0.12 release 2014-02-26
--------------------------------------------------------------------------------

  - Bugfix release + shell cleanup

Version 0.11 release 2014-02-25
--------------------------------------------------------------------------------

  - Closed tickets:
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=16&state=closed

Version 0.10 release 2014-01-18
--------------------------------------------------------------------------------

  - Changed versioning scheme from major.minor.patch to major.minor
    due to Python's messed up installers
  - Fixed job script cleanup:
    https://github.com/radical-cybertools/radical.saga/issues?milestone=15&state=closed

Version 0.9.16 release 2014-01-13
--------------------------------------------------------------------------------

  - Some improvements to sftp file adaptor
  - Closed tickets:
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=14&state=closed

Version 0.9.15 release 2013-12-10
--------------------------------------------------------------------------------

  - Emergency release to fix missing VERSION file

Version 0.9.14 release 2013-12-10
--------------------------------------------------------------------------------

  - Migration to radical.utils
  - Numerous SFTP file adaptor improvements
  - Closed tickets:
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=13&state=closed

Version 0.9.13 release 2013-11-26
--------------------------------------------------------------------------------

  - Added Platform LSF adaptor
  - Closed tickets:
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=11&state=closed

Version 0.9.12 release 2013-10-18
--------------------------------------------------------------------------------

  - Added iRODS replica adaptor
  - Closed tickets:
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=10&state=closed

Version 0.9.11 released 2013-09-04
--------------------------------------------------------------------------------

  - Bugfix release
  - Closed tickets:
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=9&state=closed

Version 0.9.10 released 2013-08-12
--------------------------------------------------------------------------------

  - Better support for Amazon EC2
  - Fixed working directory handling for PBS
  - Closed tickets:
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=3&state=closed

Version 0.9.9 released 2013-07-19
--------------------------------------------------------------------------------

  - Hotfix release: bug in Url.__str__ and SFTP copy
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=8&state=closed

Version 0.9.8 released 2013-06-22
--------------------------------------------------------------------------------

  - Hotfix release: critical bug in wait() signature
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=7&state=closed

Version 0.9.7 released 2013-06-19
--------------------------------------------------------------------------------

  - Added resource package
  - Added 'liblcoud' based adaptor to access Amazon EC2 clouds
  - Closed issues:
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=5&state=closed

Version 0.9.6 released 2013-06-17
--------------------------------------------------------------------------------

  - Hotfix release: critical PBS/TORQUE adaptor fixes
    - https://github.com/radical-cybertools/radical.saga/issues?&milestone=6&state=closed

Version 0.9.5 released 2013-06-06
--------------------------------------------------------------------------------

  - Hotfix release: critical SLURM adaptor fixes
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=4&state=closed

Version 0.9.4 released 2013-06-01
--------------------------------------------------------------------------------

  - jd.working_directory now gets created if it doesn't exist
  - Support for older Cray systems running PBS Pro 10
  - Job state callback support for the PBS adaptor - others to follow
  - A simple HTTP protocol file adaptor
  - Fixed some issues with user-pass and X.509 security contexts
  - Over 40 bugfixes and improvements:
    - https://github.com/radical-cybertools/radical.saga/issues?milestone=2&state=closed

Version 0.9.3 released 2013-04-08
--------------------------------------------------------------------------------

  - Added SFTP adaptor
  - Added tutorial examples
  - Closed issues:
    - https://github.com/radical-cybertools/radical.saga/issues/78
    - https://github.com/radical-cybertools/radical.saga/issues/73
    - https://github.com/radical-cybertools/radical.saga/issues/72
    - https://github.com/radical-cybertools/radical.saga/issues/71
    - https://github.com/radical-cybertools/radical.saga/issues/69
    - https://github.com/radical-cybertools/radical.saga/issues/66
    - https://github.com/radical-cybertools/radical.saga/issues/63
    - https://github.com/radical-cybertools/radical.saga/issues/62
    - https://github.com/radical-cybertools/radical.saga/issues/61
    - https://github.com/radical-cybertools/radical.saga/issues/60
    - https://github.com/radical-cybertools/radical.saga/issues/58
    - https://github.com/radical-cybertools/radical.saga/issues/57
    - https://github.com/radical-cybertools/radical.saga/issues/56
    - https://github.com/radical-cybertools/radical.saga/issues/55
    - https://github.com/radical-cybertools/radical.saga/issues/22
    - https://github.com/radical-cybertools/radical.saga/issues/51
    - https://github.com/radical-cybertools/radical.saga/issues/53
    - https://github.com/radical-cybertools/radical.saga/issues/26
    - https://github.com/radical-cybertools/radical.saga/issues/49
    - https://github.com/radical-cybertools/radical.saga/issues/50
    - https://github.com/radical-cybertools/radical.saga/issues/47
    - https://github.com/radical-cybertools/radical.saga/issues/45
    - https://github.com/radical-cybertools/radical.saga/issues/46
    - https://github.com/radical-cybertools/radical.saga/issues/43
    - https://github.com/radical-cybertools/radical.saga/issues/27

Version 0.9.2 released 2013-03-11
--------------------------------------------------------------------------------

  - Hotfix release

Version 0.9.1 released 2013-03-03
--------------------------------------------------------------------------------

  - Major re-write of engine and adaptor interface
  - Support for asynchronous operations
  - Improved PTYWrapper for ssh/gsissh remote execution
  - Added SLURM job adaptor
  - Added Condor job adaptor

Version 0.2.7 released 2012-11-09
--------------------------------------------------------------------------------

  - Fixed errors related to pbs://localhost and sge://localhost
  URLs that were caused by a bug in the command-line wrappers.

Version 0.2.6 released 2012-10-25
--------------------------------------------------------------------------------

  - HOTFIX: credential management for SGE and PBS. both adaptors now
    iterate over SSH and GSISSH contexts as well as consider usernames
    that are part of the url, e.g., pbs+ssh://ole@lonestar.tacc...

Version 0.2.5 released 2012-10-24
--------------------------------------------------------------------------------

  - Changed documentation to Sphinx
  - Removed object_type API. Python buildins can be used instead
  - Updates to Filesystem API
  - Added JobDescription.name attribute (as defined in DRMAA)
  - Introduced stateful SSH connection substrate for PBS, SGE, etc
  - Introduced support for GSISSH: pbs+gsissh://, sge+gsissh://
  - Re-implementation of a (more Python-esque) attribute interface
  - Fixed JobID issues, i.e., job.job_id returns 'None' in case the
    job is not running instead of
  - Introduced dynamic, fault-tolerant plug-in loader. If anything
    goes wrong during loading of a specific plug-in (i.e.,
    dependencies on 3rd party modules cannot be fulfilled, the plug-in
    will just get skipped and the remaining ones will still get
    loaded. Previously, a single problem during plug-in loading would
    take radical.saga down.

Version 0.2.4 released 2012-7-10
--------------------------------------------------------------------------------

  - Added unit-tests for SPMDVariation
  - Added 'mpirun' support for local job plug-in (via SPMDVariation)
  - Added some of the missing methods and flags to filesystem package
  - An URL object can now be constructed from another URL object
  - Fixed job.cancel()
  - Wildcard support for Directory.list()

Version 0.2.3 released 2012-6-26
--------------------------------------------------------------------------------

  - Fixed query support for URL class (issue #61)
  - Improved logging. No root logger hijacking anymore (issue #62)
  - Fixed job.Description.number_of_processes (issue #63)
  - Less chatty SSH plug-in (issue #51)

Version 0.2.2 released 2012-6-12
--------------------------------------------------------------------------------

  - job.Decription now accepts strings for int values. This has been
    implemented for backwards compatibility
  - Fixed resource.Compute.wait() timeout issue
  - Removed excessive SGE/PBS plug-in logging
  - job.Service can now be created from a resource.Manager
  - Implemented deep copy for description objects
  - Runtime now supports multiple plug-ins for the same schema

Version 0.2.1 released 2012-5-16
--------------------------------------------------------------------------------

  - Fixed https://github.com/radical-cybertools/radical.saga/issues/5
  - Fixed https://github.com/radical-cybertools/radical.saga/issues/13

Version 0.2.0 released 2012-5-15
--------------------------------------------------------------------------------

  - SFTP support for local <-> remote copy operations, mkdir, get_size
  - Added supoprt for ssh re-connection after timeout (issue #29)
  - Abandoned 'Exception' filenames and API inheritance. The radical.saga
    interface looks much cleaner now. Compatibility with previous
    versions has been ensured
  - Improved (inline) API documentation
  - Swapped urlparse with furl in saga.Url class This hopefully fixes
    the problem with inconsistent parsing accross different Python
    versions
  - Added SGE (Sun Grid Engine) plug-in (issue #11)
  - Removed sagacompat compatibility API
  - Log source names now all start with 'radical.saga.'. This should make
    filtering much easier
  - Moved SD package into development branch features/servicediscovery

Version 0.1.19 released 2012-02-29
--------------------------------------------------------------------------------

  - Hotfix - removed experimental Resource plug-in from release

Version 0.1.18 released 2012-02-29
--------------------------------------------------------------------------------

  - Fixed issue with plugin introspection
  - Added template for job plug-in

Version 0.1.17 released 2012-01-04
--------------------------------------------------------------------------------

  - Hotfix

Version 0.1.16 released 2012-01-03
--------------------------------------------------------------------------------

  - Fixed issue: https://github.com/oweidner/radical.saga/issues/9

Version 0.1.15 released 2012-01-03
--------------------------------------------------------------------------------

  - Fixed issue: https://github.com/oweidner/radical.saga/issues/8
  - Fixed issue: https://github.com/oweidner/radical.saga/issues/6
  - First version of a bigjob plugin. See wiki for details.
  - Fixed Python 2.4 compatibility issue

Version 0.1.14 released 2011-12-08
--------------------------------------------------------------------------------

  - Added bliss.sagacompat module for API compatibility.  
    - API documentation:
      http://oweidner.github.com/bliss/apidoc-compat/
  - Added examples for 'compat' API, e.g.:
    https://github.com/oweidner/bliss/tree/master/examples/job-api/compat/
  - Added configuration files for epydoc

Version 0.1.13 released 2011-12-07
--------------------------------------------------------------------------------

  - Fixed executable & argument handling for the local job plugin
  - Added support for jd.output and jd.error to local job plugin

Version 0.1.12 released 2011-12-06
--------------------------------------------------------------------------------

  - Fixed bug in URL.get_host()
  - Fixed issues with extremely short running PBS jobs in conjunction
    with scheduler configruations that remove the job from the queue
    the second it finishes execution.
  - First working version of an SFTP file API plugini based on
    Paramiko
  - Two advance bfast examples incl. output file staging:
    https://github.com/oweidner/bliss/blob/master/examples/advanced/bfast_workflow_01.py
    https://github.com/oweidner/bliss/blob/master/examples/advanced/bfast_workflow_02.py

Version 0.1.11 released 2011-11-28
--------------------------------------------------------------------------------

  - Fixed issues with PBS working directory
  - Added simple job API example that uses BFAST:
    https://github.com/oweidner/bliss/blob/master/examples/job-api/pbs_via_ssh_bfast_job.py
  - Updated apidoc: http://oweidner.github.com/bliss/apidoc/
  - First prototype of a job container. Example can be found here:
    https://github.com/oweidner/bliss/blob/master/examples/job-api/pbs_via_ssh_container.py  
  - Implemented CPU and Memory information via PBS service discovery
  - Changed job.Description.walltime_limit to
    job.Description.wall_time_limit

Version 0.1.10 released 2011-11-16
--------------------------------------------------------------------------------

  - Fixed issue with local job plugin

Version 0.1.9 released 2011-11-16
--------------------------------------------------------------------------------

  - Prototype of a Service Discovery packages
  - PBS/SSH support for service discovery

Version 0.1.8 released 2011-11-09
--------------------------------------------------------------------------------

  - Fixed issue with PBS plugin job.wait()

Version 0.1.7 released 2011-11-09
--------------------------------------------------------------------------------

  - More or less stable job API    
  - First functional PBS over SSH plugin


--------------------------------------------------------------------------------

