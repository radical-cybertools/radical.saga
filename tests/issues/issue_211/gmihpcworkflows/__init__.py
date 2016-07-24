
from __future__ import absolute_import
from __future__ import print_function
import saga
 
session = None
 
# just to make sure that we re-use the session (not sure if necessary)
def _get_session():
    global session
    if session is None:
        ctx = saga.Context("ssh")
        ctx.user_id = USER
        session = saga.Session(default=False)
        session.add_context(ctx)
    return session
    
def run_gwa(id):
    try:
        cluster_ssh_js = None
        cluster_sge_js = None
        DATA_FOLDER = 'SOMEFOLDER'
        INPUT_FOLDER = '%s/INPUT' % DATA_FOLDER
        OUTPUT_FOLDER = '%s/OUTPUT' % DATA_FOLDER
        LOG_FOLDER = '%s/LOG/' % DATA_FOLDER
        data = _get_data_from_REST(id)
        
        # write out csv file that was retrieved via REST call
        fd, phenotype_file_path = tempfile.mkstemp()
        os.write(fd, phenotype_data)
        session = _get_session()
        
        # STAGE IN INPUT FILES
        cluster_ssh_js = saga.job.Service("ssh://%s" % (HPC_HOST), session=session)
        # create folders on HPC cluster
        j = cluster_ssh_js.run_job("mkdir -p DATA/%s/INPUT" % studyid)
        # copy files to HPC cluster
        phenotype_file = saga.filesystem.File('local://%s' % phenotype_file_path,session=session)
        phenotype_file.copy('sftp://%s/%s/%s.csv' % (HPC_HOST,INPUT_FOLDER,studyid))
        
        # RUN JOB
        CLUSTER_URL =  "%s+ssh://%s" % (HPC_SCHEDULER,HPC_HOST)
        cluster_sge_js = saga.job.Service(CLUSTER_URL, session=session)
        gwas_jd = saga.job.Description()
        gwas_jd.executable = '%s/analysis.sh' %  SCRIPTS_FOLDER
        gwas_jd.working_directory = DATA_FOLDER
        gwas_jd.arguments = [id, '1']
        gwas_jd.spmd_variation = 'threads'
        gwas_jd.output = '/%s/DATA/LOG/RUN.out' % HOME_FOLDER
        gwas_jd.error       = '%s/DATA/LOG/RUN.err' % HOME_FOLDER
        gwas_jd.queue = 'q.norm'
        gwas_jd.project='SOME PROJECT'
 
        gwas_job = cluster_sge_js.create_job(gwas_jd)
        gwas_job.run()
        pattern = "\[%s\+ssh://%s\]\-\[([0-9]+)\]" % (HPC_SCHEDULER,HPC_HOST)
        gwas_job_id_match = re.match(pattern,gwas_job.id)
        if not gwas_job_id_match:
            raise Exception('Failed to get jobid %s' % gwas_job.id)
        gwas_job_id = gwas_job_id_match.group(1)
        return {'saga_job_id':gwas_job.id,'sge_job_id':gwas_job_id}
    except Exception as err:
        print(str(err))
        raise err
    finally:
        if phenotype_file_path is not None:
            os.remove(phenotype_file_path)
        #if cluster_sge_js is not None:
         #   cluster_sge_js.close()
 
# check if job is finished
def check_job_state(jobid,sge_job_id,id):
    try:
        session = _get_session()
        CLUSTER_URL =  "%s+ssh://%s" % (HPC_SCHEDULER,HPC_HOST)
        cluster_sge_js = saga.job.Service(CLUSTER_URL, session=session)
        job = cluster_sge_js.get_job(jobid)
        status = job.get_state()
    except saga.NoSuccess as err: 
        if not "Couldn't reconnect to job" in err.message:
            raise err
        status = saga.job.DONE
    if status == saga.job.DONE:
        status=_check_stageout_and_cleanup(id,sge_job_id)
    return status
 
def _check_stageout_and_cleanup(id,sge_job_id):
    FAILED = 'Failed'
    session = _get_session()
    cluster_ssh_js = saga.job.Service("ssh://%s" % (HPC_HOST), session=session)
    # CHECK OUTPUT FILES
    error_file = saga.filesystem.File(str('sftp://%s/home/GMI/%s/DATA/%s/LOG/%s.err' % (HPC_HOST,USER,studyid,sge_job_id)),session=session)
    if not error_file.is_file():
        return FAILED
    elif error_file.get_size() > 0:
        return FAILED
    hdf5_file = saga.filesystem.File(str('sftp://%s/home/GMI/%s/GWASDATA/%s/OUTPUT/%s.hdf5' % (HPC_HOST,USER,studyid,studyid)),session=session)
    if not hdf5_file.is_file():
        return FAILED
    elif hdf5_file.get_size() == 0:
        return FAILED
    # STAGE OUT OUTPUT FILES AND CLEANUP
    hdf5_file.copy('file:%s/%s.hdf5' % (STUDY_DATA_FOLDER,studyid))
    hdf5_file.remove()
    j = cluster_ssh_js.run_job("rm -fr GWASDATA/%s/" % studyid)
    return saga.job.DONE
