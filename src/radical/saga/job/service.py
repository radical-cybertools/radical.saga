
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" SAGA job service interface """


import radical.utils            as ru
import radical.utils.signatures as rus

from ..constants import SYNC, ASYNC, TASK
from ..adaptors  import base    as sab

from .. import task             as st
from .. import base             as sb
from .. import sasync
from .. import exceptions       as se
from .. import session          as ss

from .  import job              as j
from .  import description      as descr


# ------------------------------------------------------------------------------
#
class Service (sb.Base, sasync.Async) :
    """
    The job.Service represents a resource management backend, and as such allows
    the creation, submission and management of jobs.

    A job.Service represents anything which accepts job creation requests, and
    which manages thus created :class:`saga.job.Job` instances.  That can be
    a local shell, a remote ssh shell, a cluster queuing system, a IaaS backend
    -- you name it.

    The job.Service is identified by an URL, which usually points to the contact
    endpoint for that service.


    Example::

        service  = saga.job.Service("fork://localhost")
        ids = service.list()

        for job_id in ids :
            print(job_id)

            j = service.get_job(job_id)

            if j.get_state() == saga.job.Job.Pending:
                print("pending")
            elif j.get_state() == saga.job.Job.Running:
                print("running")
            else:
                print("job is already final!")

        service.close()
    """

    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Service',
                  rus.optional ((str, ru.Url)),
                  rus.optional (ss.Session),
                  rus.optional (sab.Base),
                  rus.optional (dict),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (rus.nothing)
    def __init__ (self, rm=None, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) :
        """
        __init__(rm, session)

        Create a new job.Service instance.

        :param rm:      resource manager URL
        :type  rm:      string or :class:`saga.Url`
        :param session: an optional session object with security contexts
        :type  session: :class:`saga.Session`
        :rtype:         :class:`saga.job.Service`
        """

        # job service instances are resource hogs.  Before attempting to create
        # a new instance, we attempt to clear out all old instances.   There is
        # some collateral damage: we cannot run the Python GC over only the
        # job.Service instances, but have to run it globally -- however,
        # compared to the latency introduced by the job service setup, this
        # should be a minor inconvenienve (tm)
        try :
            import gc
            gc.collect ()

        except :
            pass


        # param checks
        self.valid  = False
        url         = ru.Url (rm)

        if  not url.scheme :
            url.scheme = 'fork'

        if  not url.host :
            url.host = 'localhost'

        if  not session :
            session = ss.Session (default=True)

        scheme = url.scheme.lower ()

        self._super = super  (Service, self)
        self._super.__init__ (scheme, _adaptor, _adaptor_state,
                              url, session, ttype=_ttype)

        self.valid  = True

    # --------------------------------------------------------------------------
    #
    @classmethod
    @rus.takes   ('Service',
                  rus.optional ((ru.Url, str)),
                  rus.optional (ss.Session),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (st.Task)
    def create   (cls, rm=None, session=None, ttype=SYNC) :
        """
        create(rm=None, session=None)
        Create a new job.Service instance asynchronously.

        :param rm:      resource manager URL
        :type  rm:      string or :class:`saga.Url`
        :param session: an optional session object with security contexts
        :type  session: :class:`saga.Session`
        :rtype:         :class:`saga.Task`
        """

        # param checks
        if not session :
            session = ss.Session (default=True)

        url = ru.Url (rm)

        return cls (url, session, _ttype=ttype)._init_task


    # --------------------------------------------------------------------------
    #
    @rus.takes     ('Service')
    @rus.returns   (str)
    def __str__ (self):
        """
        __str__()

        String representation. Returns the job service Url.
        """

        if  self.valid :
            return "[%s]" % self.url

        return ""


    # --------------------------------------------------------------------------
    #
    @rus.takes     ('Service')
    @rus.returns   (rus.nothing)
    def close (self) :
        """
        close()

        Close the job service instance and disconnect from the (remote)
        job service if necessary. Any subsequent calls to a job service
        instance after `close()` was called will fail.

        Example::

            service = saga.job.Service("fork://localhost")

            # do something with the 'service' object, create jobs, etc...

            service.close()

            service.list() # this call will throw an exception


        .. warning:: While in principle the job service destructor calls
            `close()` automatically when a job service instance goes out of
            scope, you **shouldn't rely on it**. Python's garbage collection can
            be a bit odd at times, so you should always call `close()`
            explicitly.  Especially in a **multi-threaded program** this will
            help to avoid random errors.  """

        if not self.valid :
            raise se.IncorrectState ("This instance was already closed.")

        self._adaptor.close ()
        self.valid = False


    # --------------------------------------------------------------------------
    #
    @rus.takes     ('Service',
                    descr.Description,
                    rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns   ((j.Job, st.Task))
    def create_job (self, job_desc, ttype=None) :
        """
        create_job(job_desc)

        Create a new job.Job instance from a :class:`~saga.job.Description`. The
        resulting job instance is in :data:`~saga.job.NEW` state.

        :param job_desc: job description to create the job from
        :type job_desc:  :data:`saga.job.Description`
        :param ttype: |param_ttype|
        :rtype:       :class:`saga.job.Job` or |rtype_ttype|

        create_job() accepts a job description, which described the
        application instance to be created by the backend.  The create_job()
        method is not actually attempting to *run* the job, but merely parses
        the job description for syntactic and semantic consistency.  The job
        returned object is thus not in 'Pending' or 'Running', but rather in
        'New' state.  The actual submission is performed by calling run() on
        the job object.


        Example::

            # A job.Description object describes the executable/application
            # and its requirements
            job_desc = saga.job.Description()
            job_desc.executable  = '/bin/sleep'
            job_desc.arguments   = ['10']
            job_desc.output      = 'myjob.out'
            job_desc.error       = 'myjob.err'

            service = saga.job.Service('local://localhost')

            job = service.create_job(job_desc)

            # Run the job and wait for it to finish
            job.run()
            print("Job ID    : %s" % (job.job_id))
            job.wait()

            # Get some info about the job
            print("Job State : %s" % (job.state))
            print("Exitcode  : %s" % (job.exit_code))

            service.close()
        """


        if not self.valid :
            raise se.IncorrectState ("This instance was already closed.")

        jd_copy = descr.Description()
        job_desc._attributes_deep_copy (jd_copy)

        # do some sanity checks: if the adaptor has specified a set of supported
        # job description attributes, we scan the given description for any
        # mismatches, and complain then.
        adaptor_info = self._adaptor._adaptor.get_info ()

        if  'capabilities'    in adaptor_info             and \
            'jdes_attributes' in adaptor_info['capabilities'] :

            # this is the list of key supported by the adaptor.  These
            # attributes may be set to non-default values
            supported_keys = adaptor_info['capabilities']['jdes_attributes']

            # use an empty job description to compare default values
            jd_default = descr.Description ()

            for key in jd_copy.list_attributes () :

                val     = jd_copy   .get_attribute (key)
                default = jd_default.get_attribute (key)

                # Also, we make string compares case insensitive
                if isinstance (val,     str) : val     = val    .lower ()
                if isinstance (default, str) : default = default.lower ()

                # supported keys are also valid, as are keys with default or
                # None values
                if  key not in supported_keys and \
                    val != default            and \
                    val                       :

                    msg = "'JobDescription.%s' (%s) not supported by %s" \
                        % (key, val, adaptor_info['name'])
                    raise se.BadParameter._log (self._logger, msg)


        # make sure at least 'executable' is defined
        if jd_copy.executable is None:
            raise se.BadParameter("No executable defined")

        # convert environment to string
        if jd_copy.attribute_exists ('Environment') :
            for (key, value) in jd_copy.environment.items():
                jd_copy.environment[key] = str(value)

        return self._adaptor.create_job (jd_copy, ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Service',
                  str,
                  rus.optional (str),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((j.Job, st.Task))
    def run_job  (self, cmd, host=None, ttype=None) :
        """
        run_job(cmd, host=None)
        """

        if not self.valid :
            raise se.IncorrectState ("This instance was already closed.")

        if not cmd:
            raise se.BadParameter('run_job needs a command to run.  Duh!')

        try:
            # lets see if the adaptor implements run_job
            return self._adaptor.run_job (cmd, host, ttype=ttype)
        except:
            # fall back to the default implementation below
            pass

        # The adaptor has no run_job -- we here provide a generic implementation
        # FIXME: split should be more clever and respect POSIX shell syntax.
        args = cmd.split()

        jd = descr.Description()
        jd.executable = args[0]
        jd.arguments  = args[1:]

        job = self.create_job(jd)
        job.run()

        return job


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Service',
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.list_of (str), st.Task))
    def list     (self, ttype=None) :
        """
        list()

        Return a list of the jobs that are managed by this Service
        instance.

        .. seealso::
           The :data:`~saga.job.Service.jobs` property and the
           :meth:`~saga.job.Service.list` method are semantically
           equivalent.

        :ttype: |param_ttype|
        :rtype: list of :class:`saga.job.Job`

        As the job.Service represents a job management backend, list() will
        return a list of job IDs for all jobs which are known to the backend,
        and which can potentially be accessed and managed by the application.


        Example::

            service  = saga.job.Service("fork://localhost")
            ids = service.list()

            for job_id in ids :
                print(job_id)

            service.close()
        """

        if not self.valid :
            raise se.IncorrectState ("This instance was already closed.")


        return self._adaptor.list (ttype=ttype)

    jobs = property (list)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Service',
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((ru.Url, st.Task))
    def get_url  (self, ttype=None) :
        """
        get_url()

        Return the URL this Service instance was created with.

        .. seealso::
           The :data:`~saga.job.Service.url` property and the
           :meth:`~saga.job.Service.get_url` method are semantically
           equivalent and only duplicated for convenience.
        """

        if not self.valid :
            raise se.IncorrectState ("This instance was already closed.")

        return self._adaptor.get_url (ttype=ttype)

    url = property (get_url)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Service',
                  str,
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((j.Job, st.Task))
    def get_job  (self, job_id, ttype=None) :
        """
        get_job(job_id)

        Return the job object for a given job id.

        :param job_id: The id of the job to retrieve
        :rtype:        :class:`saga.job.Job`


        Job objects are a local representation of a remote stateful entity.
        The job.Service supports to reconnect to those remote entities::

            service = saga.job.Service("fork://localhost")
            j  = service.get_job(my_job_id)

            if j.get_state() == saga.job.Job.Pending:
                print("pending")
            elif j.get_state() == saga.job.Job.Running:
                print("running")
            else:
                print("job is already final!")

            service.close()
        """

        if not self.valid :
            raise se.IncorrectState ("This instance was already closed.")

        return self._adaptor.get_job (job_id, ttype=ttype)

# FIXME: add get_self()

# ------------------------------------------------------------------------------

