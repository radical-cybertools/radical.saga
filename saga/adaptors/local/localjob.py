
def register () :

    registry = [{ 'name'    : 'local job adaptor',
                  'type'    : 'saga.job.Job',
                  'class'   : 'local_job',
                  'schemas' : ['fork', 'local']
                }, 
                { 'name'    : 'local job adaptor',
                  'type'    : 'saga.job.Service',
                  'class'   : 'local_job_service',
                  'schemas' : ['fork', 'local']
                }]

    return registry


class local_job (object) :

    def __init__ (self) :
        print "local job adaptor init";


class local_job_service (object) :

    def __init__ (self) :
        print "local job service adaptor init";

