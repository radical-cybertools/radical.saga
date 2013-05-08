import sys
import saga

REMOTE_HOST = "ssh://gw68.quarry.iu.teragrid.org"

def main():
    try:

        for i in range(0, 10):
            print "************************ Job: %d *************************" % i
            ctx = saga.Context("ssh")

            session = saga.Session()
            session.add_context(ctx)

            # Create a job service object that represent a remote pbs cluster.
            # The keyword 'pbs' in the url scheme triggers the PBS adaptors
            # and '+ssh' enables PBS remote access via SSH.
            js = saga.job.Service(REMOTE_HOST, session=session)

            # describe our job
            jd = saga.job.Description()

            # Next, we describe the job we want to run. A complete set of job
            # description attributes can be found in the API documentation.
            #jd.environment     = {'MYOUTPUT':'"Hello from SAGA"'}
            #jd.environment     = {'MYOUTPUT':'"Hello from SAGA"'}
            jd.executable      = '/bin/date'
            #jd.queue      = 'normal'
            #jd.project      = 'TG-MCB090174'
            jd.wall_time_limit      = '10'
            #jd.arguments       = ['$MYOUTPUT']
            jd.output          = "/tmp/mysagajob.stdout"
            jd.error           = "/tmp/mysagajob.stderr"

            # Create a new job from the job description. The initial state of
            # the job is 'New'.
            myjob = js.create_job(jd)

            # Check our job's id and state
            print "Job ID    : %s" % (myjob.id)
            print "Job State : %s" % (myjob.state)

            print "\n...starting job...\n"

            # Now we can start our job.
            myjob.run()

            print "Job ID    : %s" % (myjob.id)
            print "Job State : %s" % (myjob.state)


        return 0

    except saga.SagaException, ex:
        # Catch all saga exceptions
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Trace back the exception. That can be helpful for debugging.
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1


if __name__ == "__main__":
    sys.exit(main())