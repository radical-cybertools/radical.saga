
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
        print "local job adaptor init";


    def _init_sync (self) :
        print "local job adaptor init sync"



    def get_id (self) :

        return id (self)


######################################################################
#
# job service adaptor class
#
class local_job_service (saga.cpi.job.Service) :

    def __init__ (self) :
        print "local job service adaptor init"


    def _init_sync (self, rm, session) :
        print "local job service adaptor init sync: %s"  %  rm 
        self._rm      = rm
        self._session = session


    def create (self, rm, session, ttype) :
        pass


    def get_url (self, ttype) :

        return self._rm


