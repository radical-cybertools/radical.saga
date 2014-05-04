
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" This examples shows how to run a job on a remote Condor gateway
    using the 'condor' job adaptor.
"""

import sys
import saga
import getpass


def main():
    try:
        # Your ssh identity on the remote machine.
        ctx = saga.Context("ssh")
        ctx.user_id = getpass.getuser()  # Change if necessary

        session = saga.Session()
        session.add_context(ctx)

        # create a job service for the local machine. both, 'fork' and
        # 'local' schemes trigger the local job adaptor.
        js = saga.job.Service("condor+ssh://gw68.quarry.iu.teragrid.org?WhenToTransferOutput=ON_EXIT&should_transfer_files=YES&notification=Always",
                              session=session)

        # describe our job
        jd = saga.job.Description()

        # environment, executable & arguments. We use '/bin/sleep' to simulate
        # a job that runs for $RUNTIME seconds.
        jd.name            = 'testjob'
        jd.project         = 'TG-MCB090174'
        jd.environment     = {'RUNTIME': '/etc/passwd'}
        jd.wall_time_limit = 2 # minutes

        jd.executable = '/bin/cat'
        jd.arguments = ["$RUNTIME"]

        jd.output          = "saga_condorjob.stdout"
        jd.error           = "saga_condorjob.stderr"
 
 #       jd.candidate_hosts = ["FNAL_FERMIGRID", "cinvestav", "SPRACE",
 #                             "NYSGRID_CORNELL_NYS1", "Purdue-Steele",
 #                             "MIT_CMS_CE2", "SWT2_CPB", "AGLT2_CE_2",
 #                             "UTA_SWT2", "GridUNESP_CENTRAL",
 #                             "USCMS-FNAL-WC1-CE3"]

        # create the job (state: New)
        sleepjob = js.create_job(jd)

        # check our job's id and state
        print "Job ID    : %s" % (sleepjob.id)
        print "Job State : %s" % (sleepjob.state)

        print "\n...starting job...\n"
        sleepjob.run()

        print "Job ID    : %s" % (sleepjob.id)
        print "Job State : %s" % (sleepjob.state)

        print "\nListing active jobs: "
        for job in js.list():
            print " * %s" % job

        # disconnect / reconnect
        sleebjob_clone = js.get_job(sleepjob.id)

        # wait for our job to complete
        print "\n...waiting for job...\n"
        sleebjob_clone.wait()

        print "Job State   : %s" % (sleebjob_clone.state)
        print "Exitcode    : %s" % (sleebjob_clone.exit_code)
        print "Exec. hosts : %s" % (sleebjob_clone.execution_hosts)
        print "Create time : %s" % (sleebjob_clone.created)
        print "Start time  : %s" % (sleebjob_clone.started)
        print "End time    : %s" % (sleebjob_clone.finished)

        js.close()
        return 0

    except saga.SagaException, ex:
        print "An exception occured: %s " % ((str(ex)))
        # get the whole traceback in case of an exception -
        # this can be helpful for debugging the problem
        print " *** %s" % ex.traceback
        return -1

if __name__ == "__main__":
    sys.exit(main())
