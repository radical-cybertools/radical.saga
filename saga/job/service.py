
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" SAGA job service interface
"""


from saga.base            import Base
from saga.async           import Async
from saga.url             import Url
from saga.job.description import Description
from saga.exceptions      import BadParameter

from saga.constants       import SYNC, ASYNC, TASK, NOTASK # task constants


class Service (Base, Async) :
    """ The job.Service represents a resource management backend, and as 
        such allows the creation, submission and management of jobs.

        :param url:     resource manager URL
        :type  url:     string or :class:`saga.Url`
        :param session: an optional session object with security contexts
        :type  session: :class:`saga.Session`
        :rtype:         :class:`saga.job.Service`

    """

    def __init__ (self, url=None, session=None, 
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        """ Create a new job.Service instance.
        """

        # param checks
        url     = Url (url)
        scheme  = url.scheme.lower ()

        Base.__init__ (self, scheme, _adaptor, _adaptor_state, 
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
        url     = Url (url)
        scheme  = url.scheme.lower ()

        return cls (url, flags, session, _ttype=ttype)._init_task


    def create_job (self, job_desc, ttype=None) :
        """ Create a new job.Job instance from a 
            :class:`~saga.job.Description`. The resulting job instance
            is in :data:`~saga.job.NEW` state. 

            :param job_desc: job description to create the job from
            :type job_desc:  :data:`saga.job.Description`
            :param ttype: |param_ttype|
            :rtype:       :class:`saga.job.Job` or |rtype_ttype|
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
        """ Return a list of the jobs that are managed by this Service 
            instance. 

            .. seealso:: 
               The :data:`~saga.job.Service.jobs` property and the
               :meth:`~saga.job.Service.list` method are semantically 
               equivalent and only duplicated for convenience.

            :ttype: |param_ttype|
            :rtype: list of :class:`saga.job.Job`
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
        """ Return the job object for a given job id.

            :param job_id: The id of the job to retrieve
            :rtype:     :class:`saga.job.Job`
        """
        return self._adaptor.get_job (job_id, ttype=ttype)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

