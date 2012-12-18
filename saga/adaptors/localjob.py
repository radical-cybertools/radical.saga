
import saga.cpi.base
import saga.cpi.job

######################################################################
#
# adaptor meta data
#
_adaptor_name     =    'saga.adaptor.localjob'
_adaptor_registry = [{ 'name'    : _adaptor_name,
                       'type'    : 'saga.job.Job',
                       'class'   : 'local_job',
                       'schemas' : ['fork', 'local']
                     }, 
                     { 'name'    : _adaptor_name,
                       'type'    : 'saga.job.Service',
                       'class'   : 'local_job_service',
                       'schemas' : ['fork', 'local']
                     }]


######################################################################
#
# adaptor registration
#
def register () :

    # perform some sanity checks, like check if dependencies are met
    if "today" == "Monday" :
        return None

    return _adaptor_registry


######################################################################
#
# job adaptor class
#
class local_job (saga.cpi.job.Job) :

    def __init__ (self) :
        saga.cpi.Base.__init__ (self, _adaptor_name)
        print "local job adaptor init";


    def init_instance (self, id, session) :
        print "local job adaptor init sync"
        self._id      = id
        self._session = session


    def get_id (self) :
        return id (self)


######################################################################
#
# job service adaptor class
#
class local_job_service (saga.cpi.job.Service) :

    def __init__ (self) :
        saga.cpi.Base.__init__ (self, _adaptor_name)
        print "local job service adaptor init"


    def init_instance (self, rm, session) :
        print "local job service adaptor init sync: %s"  %  rm 
        self._rm      = rm
        self._session = session


        # for testing:
        # raise saga.exceptions.BadParameter ("Cannot handle rm %s"  %  rm)


    def init_instance_async (self, ttype, rm, session) :
        print "local job service adaptor init async: %s"  %  rm 
        self._rm      = rm
        self._session = session

        # for testing:
        # raise saga.exceptions.BadParameter ("Cannot handle rm %s"  %  rm)

        # FIXME: we need to return a saga.task.Task instance here
        t = saga.task.Task ()
        return t


    def get_url (self, ttype) :

        return self._rm


    def create_job (self, jd, ttype) :
        print jd._attributes_dump ()
        j = saga.job._create_job_from_adaptor ("my_id", self._session, "fork", _adaptor_name)
        return j

