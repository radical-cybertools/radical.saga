import sys
import saga

USER_ID     = "oweidner"
REMOTE_HOST = "gw68.quarry.iu.teragrid.org"

def main () :
    try:

        for i in range(0, 1000):
            print "**************************** Job: %d *****************************" % i
            ctx = saga.Context("ssh")
            ctx.user_id = USER_ID

            session = saga.Session()
            session.add_context(ctx)

            # Create a job service object that represent a remote pbs cluster.
            # The keyword 'pbs' in the url scheme triggers the PBS adaptors
            # and '+ssh' enables PBS remote access via SSH.
            js = saga.job.Service("ssh://%s" % REMOTE_HOST, session=session) 

            # describe our job
            jd = saga.job.Description()

            # Next, we describe the job we want to run. A complete set of job
            # description attributes can be found in the API documentation.
            #jd.environment     = {'MYOUTPUT':'"Hello from SAGA"'}
            #jd.environment     = {'MYOUTPUT':'"Hello from SAGA"'}
            jd.executable       = '/bin/date'
            jd.queue            = 'normal'
            jd.project          = 'TG-MCB090174'
            jd.wall_time_limit  = '10'
            jd.total_cpu_count  = 1
            #jd.number_of_processes = 1
            #jd.arguments       = ['$MYOUTPUT']
            jd.output           = "/tmp/saga_job.%s.stdout" % USER_ID
            jd.error            = "/tmp/saga_job.%s.stderr" % USER_ID

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


            for jid in js.jobs :
                j = js.get_job (jid)
                print "%s\t " % j.state,
            print

            js.close ()
    
    
        return 0

    except saga.SagaException, ex:
        # Catch all saga exceptions
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Trace back the exception. That can be helpful for debugging.
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1


if __name__ == "__main__":
    sys.exit(main())

