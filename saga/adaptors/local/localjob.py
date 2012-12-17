
from saga.engine.config import Configurable

######################################################################
#
# adaptor meta data
#
_adaptor_name     =    'saga.adaptor.local.localjob'
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
class local_job (Configurable) :

    def __init__ (self) :
        print "local job adaptor init";


    def get_id (self) :

        return id (self)


######################################################################
#
# job service adaptor class
#
class local_job_service (Configurable) :

    def __init__ (self, url) :
        print "local job service adaptor init";

        # keep initialization state
        self._url = url


    def get_id (self) :

        return self._url


