
from .celery import Celery

celery = Celery('gmihpcworkflows', include=['gmihpcworkflows.hpc_tasks'])
 
if __name__ == '__main__':
    celery.start()

