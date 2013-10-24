
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" This is an example which shows how to access Amazon EC2 clouds via the SAGA
    resource package.

    In order to run this example, you need to set the following environment
    variables:

    * EC2_ID:           Your Amazon EC2 ID
    * EC2_KEY:          You Amazon EC2 KEY
    * EC2_SSH_KEYPAIR:  The SSH keypair you want to use to access the VM, e.g.,
                        /home/username/.ssh/rsa_ec2
                        (You can create a new temporary one using 'ssh-keygen')
"""

import os
import sys
import saga
import time


def main():

    # In order to connect to EC2, we need an EC2 ID and KEY. We read those
    # from the environment.
    ec2_ctx = saga.Context('EC2')
    ec2_ctx.user_id = os.environ['EC2_ID']
    ec2_ctx.user_key = os.environ['EC2_KEY']

    # The SSH keypair we want to use the access the EC2 VM. If the keypair is
    # not yet registered on EC2 saga will register it automatically.
    ec2keypair_ctx = saga.Context('EC2_KEYPAIR')
    ec2keypair_ctx.token = 'TODO'  # keypair name
    ec2keypair_ctx.user_key = os.environ['EC2_SSH_KEYPAIR']
    ec2keypair_ctx.user_id = 'root'  # the user id on the target VM

    # TODO
    ssh_ctx = saga.Context('SSH')
    ssh_ctx.user_id = 'root'
    ssh_ctx.user_key = os.environ['EC2_SSH_KEYPAIR']

    session = saga.Session(False)  # FALSE: don't use other (default) contexts
    session.contexts.append(ec2_ctx)
    session.contexts.append(ec2keypair_ctx)
    session.contexts.append(ssh_ctx)

    try:
        ######################################################################
        ##  STEP 1: Start a VM on EC2                                       ##
        ######################################################################

        # in this session, connect to the EC2 resource manager
        rm = saga.resource.Manager("ec2://aws.amazon.com/", session=session)

        # Create a resource description with an image and an OS template,.
        # We pick a small VM and a plain Ubuntu image...
        cd = saga.resource.ComputeDescription()
        cd.image = 'ami-0256b16b'
        cd.template = 'Small Instance'

        # Create a VM instance from that description.
        cr = rm.acquire(cd)

        print "\nWaiting for VM to become active..."

        # Wait for the VM to 'boot up', i.e., become 'ACTIVE'
        if cr.state != saga.resource.ACTIVE:
            cr.wait(saga.resource.ACTIVE)

        # Query some information about the newly created VM
        print "\nCreated VM: %s" % cr.id
        print "  state  : %s (%s)" % (cr.state, cr.state_detail)
        print "  access : %s" % cr.access

        # give the VM some time to start up comlpetely, otherwise the subsequent
        # job submission might end up failing...
        time.sleep(20)

        ######################################################################
        ##  STEP 2: Run a Job on the VM                                     ##
        ######################################################################

        js = saga.job.Service(cr.access, session=session)

        jd = saga.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['30']

        job = js.create_job(jd)
        job.run()

        print "\nRunning Job: %s" % job.id
        print "  state : %s (%s)" % (job.state)
        job.wait()
        print "  state : %s (%s)" % (job.state)

    except saga.SagaException, ex:
        # Catch all saga exceptions
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Trace back the exception. That can be helpful for debugging.
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1

    finally:

        ######################################################################
        ##  STEP 2: Shut down the VM                                        ##
        ######################################################################

        cr.destroy()

        print "\nDestroyed VM: %s" % cr.id
        print "  state : %s (%s)" % (cr.state, cr.state_detail)
        return 0

if __name__ == "__main__":
    sys.exit(main())
