#!/usr/bin/env python

__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" This examples shows how to run a job on a remote Condor gateway
    using the 'condor' job adaptor.
"""

import sys
import getpass

import radical.saga as rs


URL = "condor+ssh://gw68.quarry.iu.teragrid.org?WhenToTransferOutput=ON_EXIT&should_transfer_files=YES&notification=Always",
URL = "condor+gsissh://login.osgconnect.net"
URL = "condor+gsissh://xd-login.opensciencegrid.org"
URL = "condor+gsissh://submit-1.osg.xsede.org"

def main():
    try:
        # Your ssh identity on the remote machine.
      # ctx = rs.Context("ssh")
      # ctx.user_id = getpass.getuser()  # Change if necessary

        session = rs.Session()
      # session.add_context(ctx)

        # create a job service for the local machine. both, 'fork' and
        # 'local' schemes trigger the local job adaptor.
        js = rs.job.Service(URL, session=session)

        # describe our job
        jd = rs.job.Description()

        # environment, executable & arguments. We use '/bin/sleep' to simulate
        # a job that runs for $RUNTIME seconds.
        jd.name            = 'testjob'
        jd.project         = 'TG-MCB090174'
        jd.environment     = {'RUNTIME': '0'}
        jd.wall_time_limit = 2 # minutes

        jd.executable = '/bin/sleep'
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
        print("Job ID    : %s" % (sleepjob.id))
        print("Job State : %s" % (sleepjob.state))

        print("\n...starting job...\n")
        sleepjob.run()

        print("Job ID    : %s" % (sleepjob.id))
        print("Job State : %s" % (sleepjob.state))

        print("\nListing active jobs: ")
        for job in js.list():
            print(" * %s" % job)

        # disconnect / reconnect
        sleepjob_clone = js.get_job(sleepjob.id)

        # wait for our job to complete
        print("\n...waiting for job...\n")
        sleepjob_clone.wait()

        print("Job State   : %s" % (sleepjob_clone.state))
        print("Exitcode    : %s" % (sleepjob_clone.exit_code))
        print("Exec. hosts : %s" % (sleepjob_clone.execution_hosts))
        print("Create time : %s" % (sleepjob_clone.created))
        print("Start time  : %s" % (sleepjob_clone.started))
        print("End time    : %s" % (sleepjob_clone.finished))

        js.close()
        return 0

    except rs.SagaException as ex:
        print("An exception occured: %s " % ((str(ex))))
        # get the whole traceback in case of an exception -
        # this can be helpful for debugging the problem
        print(" *** %s" % ex.traceback)
        return -1

if __name__ == "__main__":
    sys.exit(main())
