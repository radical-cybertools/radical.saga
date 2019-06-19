
from   gmihpcworkflows.celery import celery
import gmihpcworkflows.saga_gwa as saga_gwa
 
@celery.task(serializer='json')
def start_saga(id):
    try:
        print("start saga (%s)"      % (id))
        retval = saga_gwa.run_gwa(id)
    except Exception as err:
        print("start saga (%s) : %s" % (id, err))
        _sendErrorToWebServer(id)
        raise err
 
 
@celery.task(serializer='json')
def check_saga_job(id,saga_job_id,sge_job_id):
    try:
        print("check saga job (%s, %s, %s)"      % (id, saga_job_id, sge_job_id))
        status = saga_gwa.check_job_state(saga_job_id,sge_job_id,id)
    except Exception as err:
        print("check saga job (%s, %s, %s) : %s" % (id, saga_job_id, sge_job_id, err))
        _sendErrorToWebServer(id)
        raise err

