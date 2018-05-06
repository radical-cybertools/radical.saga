#!/usr/bin/env python

__author__    = "Georgios Chantzialexiou"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import sys
import os
import saga

#-------------------------------------------------------------------
# Change REMOTE_HOST to the machine you want to run this on.
# You might have to change the URL scheme below for REMOTE_JOB_ENDPOINT
# accordingly.
REMOTE_HOST = "localhost"  # try this with different hosts

# This refers to your working directory on 'REMOTE_HOST'. If you use a\
# cluster for 'REMOTE_HOST', make sure this points to a shared filesystem.
REMOTE_DIR = "/tmp/"  # change this to your home directory

# If you change 'REMOTE_HOST' above, you might have to change 'ssh://' to e.g.,
# 'pbs+ssh://', 'sge+ssh://', depdending on the type of service endpoint on
# that particualr host.
REMOTE_JOB_ENDPOINT = "ssh://" + REMOTE_HOST

# At the moment saga-python only provides an sftp file adaptor, so changing
# the URL scheme here wouldn't make any sense.
REMOTE_FILE_ENDPOINT = "sftp://" + REMOTE_HOST + "/" + REMOTE_DIR
#----------------------------------------------------------------------


def main():
    try:

        # Your ssh identity on the remote machine
        ctx = saga.Context("ssh")
        ctx.user_id = ""
        session = saga.Session()
        session.add_context(ctx)

        # Create a job service object that represent the local machine.
        # The keyword 'fork://' in the url scheme triggers the 'shell' adaptor
        # which can execute jobs on the local machine as well as on a remote
        # machine via "ssh://hostname".
        js = saga.job.Service("fork://localhost")

        # describe our job
        jd = saga.job.Description()

        # create a working directory in /scratch
        dirname = '%s/mydir/' % (REMOTE_FILE_ENDPOINT)
        workdir = saga.filesystem.Directory(dirname, saga.filesystem.CREATE. #TODO: Create??
                                            session=session)

        ## create a dummy file
        os.system("echo 'Hello World!' > dummy.txt")
        # copy the executable and warpper script to the remote host

        dummy_file = saga.filesystem.File('file://localhost/%s/dummy.txt' % os.getcwd())
        dummy_file.copy(workdir.get_url())

        ## create a dummy folder.   
        os.system('mkdir dummy_folder') #TODO: use python and also create a file in it
        os.system("echo 'hello folder transfer!' > dummy_folder/dummy_folder_file")
        dummy_folder = saga.filesystem.File('file://localhost/%s/dummy_folder' % os.getcwd()) ## file source
        dummy_folder.copy(workdir.get_url()) # file target


        # the saga job services connects to and provides a handle
        # to a remote machine. In this case, it's your machine.
        # fork can be replaced with ssh here:
        js = saga.job.Service(REMOTE_JOB_ENDPOINT, session=session)

        # describe a single  job. we're using the
        # directory created above as the job's working directory
        jd = saga.job.Description()
        #jd.queue             = "development"
        jd.wall_time_limit   = 10
        jd.total_cpu_count   = 1
        jd.working_directory = workdir.get_url().path

        jd.environment     = {'MYOUTPUT':'"Hello from SAGA"'}
        jd.executable      = '/bin/echo'
        jd.arguments       = ['$MYOUTPUT']
        jd.output          = "/tmp/mysagajob-%s.stdout" % getpass.getuser()
        jd.error           = "/tmp/mysagajob-%s.stderr" % getpass.getuser()
        # create the job from the description
        # above, launch it and add it to the list of jobs
        job = js.create_job(jd)
        job.run()
        myjob.wait()

        for afile in workdir.list('*.txt'):
            print ' * Copying %s/%s/%s back to %s' % (REMOTE_FILE_ENDPOINT,
                                                      workdir.get_url(),
                                                      afile, os.getcwd())
            workdir.copy(afile, 'file://localhost/%s/' % os.getcwd())

        outfilesource = 'sftp://%s/tmp/mysagajob-%s.stdout' % (REMOTE_HOST, getpass.getuser())
        outfiletarget = "file://%s/" % os.getcwd()
        out = saga.filesystem.File(outfilesource, session=session)
        out.copy(outfiletarget)



        sys.exit(0)

    except saga.SagaException, ex:
        # Catch all saga exceptions
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Trace back the exception. That can be helpful for debugging.
        print " \n*** Backtrace:\n %s" % ex.traceback
        sys.exit(-1)

    except KeyboardInterrupt:
    # ctrl-c caught: try to cancel our jobs before we exit
        # the program, otherwise we'll end up with lingering jobs.
        sys.exit(-1)





if __name__ == "__main__":
    sys.exit(main())
