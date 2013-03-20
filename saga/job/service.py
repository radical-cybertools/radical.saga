
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" SAGA job service interface
"""


import saga.base         as sb
from   saga.async           import Async
from   saga.url             import Url
from   saga.job.description import Description
from   saga.exceptions      import BadParameter
from   saga.session         import Session

from   saga.constants       import SYNC, ASYNC, TASK, NOTASK # task constants


class Service (sb.Base, Async) :
    """
    The job.Service represents a resource management backend, and as such allows
    the creation, submission and management of jobs.

    :param url:     resource manager URL
    :type  url:     string or :class:`saga.Url`
    :param session: an optional session object with security contexts
    :type  session: :class:`saga.Session`
    :rtype:         :class:`saga.job.Service`

    A job.Service represents anything which accepts job creation requests, and
    which manages thus created :class:`bliss.saga.job.Job` instances.  That can be a local shell, 
    a remote ssh shell, a cluster queuing system, a IaaS backend -- you name it.

    The job.Service is identified by an URL, which usually points to the contact
    endpoint for that service.


    Example::

        my_job_id = "[fork://localhost]-[12345]"
        js  = saga.job.Service("fork://localhost")
        ids = js.list()

        if my_job_id in ids :
          print "found my job again, wohhooo!"

          j = js.get_job(my_job_id)

          if   j.get_state() == saga.job.Job.Pending  : print "pending"
          elif j.get_state() == saga.job.Job.Running  : print "running"
          else                                        : print "job is already final!"
    """

    def __init__ (self, url=None, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        """
        Create a new job.Service instance.
        
        :param url: Url of the (remote) job manager.
        :type  url: :class:`saga.Url` 
        """


        # param checks
        if not session :
            session = Session (default=True)

        url     = Url (url)
        scheme  = url.scheme.lower ()

        self._super = super  (Service, self)
        self._super.__init__ (scheme, _adaptor, _adaptor_state, 
                              url, session, ttype=_ttype)


    @classmethod
    def create (cls, url=None, session=None, ttype=SYNC) :
        """ Create a new job.Service instance asynchronously.

            :param url:     resource manager URL
            :type  url:     string or :class:`saga.Url`
            :param session: an optional session object with security contexts
            :type  session: :class:`saga.Session`
            :rtype:         :class:`saga.Task`
        """

        # param checks
        if not session :
            session = Session (default=True)

        url     = Url (url)
        scheme  = url.scheme.lower ()

        return cls (url, session, _ttype=ttype)._init_task


    def create_job (self, job_desc, ttype=None) :
        """ 
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

          js = saga.job.Service("fork://localhost")
          jd = saga.job.Description ()
          jd.executable = '/bin/date'
          j  = js.create_job(jd)

          if   j.get_state() == saga.job.Job.New      : print "new"
          else                                        : print "oops!"

          j.run()

          if   j.get_state() == saga.job.Job.Pending  : print "pending"
          elif j.get_state() == saga.job.Job.Running  : print "running"
          else                                        : print "oops!"
        """

        jd_copy = Description()
        job_desc._attributes_deep_copy (jd_copy)

        # do some sanity checks:

        # make sure at least 'executable' is defined
        if jd_copy.executable is None:
            raise BadParameter("No executable defined")

        # convert environment to string
        if jd_copy.attribute_exists ('Environment') :
            for (key, value) in jd_copy.environment.iteritems():
                jd_copy.environment[key] = str(value)

        return self._adaptor.create_job (jd_copy, ttype=ttype)


    def run_job (self, cmd, host="", ttype=None) :
        """ .. warning:: |not_implemented|
        """
        return self._adaptor.run_job (cmd, host, ttype=ttype)


    def list (self, ttype=None) :
        """ 
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

          js  = saga.job.Service("fork://localhost")
          ids = js.list()

          if my_job_id in ids :
            print "found my job again, wohhooo!"

            j = js.get_job(my_job_id)

            if   j.get_state() == saga.job.Job.Pending  : print "pending"
            elif j.get_state() == saga.job.Job.Running  : print "running"
            else                                        : print "job is already final!"
        """

        return self._adaptor.list (ttype=ttype)

    jobs = property (list)    


    def get_url (self, ttype=None) :
        """ Return the URL this Service instance was created with.

            .. seealso:: 
               The :data:`~saga.job.Service.url` property and the
               :meth:`~saga.job.Service.get_url` method are semantically 
               equivalent and only duplicated for convenience.



            :ttype: |param_ttype|
            :rtype: list of :class:`saga.job.Url`
        """
        return self._adaptor.get_url (ttype=ttype)

    url = property (get_url) 


    def get_job (self, job_id, ttype=None) :
        """ 
        Return the job object for a given job id.

        :param job_id: The id of the job to retrieve
        :rtype:     :class:`saga.job.Job`


        Job objects are a local representation of a remote stateful entity.
        The job.Service supports to reconnect to those remote entities::

          js = saga.job.Service("fork://localhost")
          j  = js.get_job(my_job_id)

          if   j.get_state() == saga.job.Job.Pending  : print "pending"
          elif j.get_state() == saga.job.Job.Running  : print "running"
          else                                        : print "job is already final!"
        """
        return self._adaptor.get_job (job_id, ttype=ttype)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

