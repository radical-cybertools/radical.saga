#!/usr/bin/env python

__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" This is an example which shows how to access Amazon EC2 clouds via the SAGA
    resource package.

    In order to run this example, you need to set the following environment
    variables:

    * EC2_ACCESS_KEY:     your Amazon EC2 ID
    * EC2_SECRET_KEY:     your Amazon EC2 KEY
    * EC2_SSH_KEYPAIR_ID: name of ssh keypair within EC2
    * EC2_SSH_KEYPAIR:    your ssh keypair to use to access the VM, e.g.,
                          /home/username/.ssh/id_rsa_ec2
"""


import os
import sys
import time

import radical.saga as rs


# ------------------------------------------------------------------------------
#
def main():

    # In order to connect to EC2, we need an EC2 ID and KEY. We read those
    # from the environment.
    ec2_ctx = rs.Context('EC2')
    ec2_ctx.user_id  = os.environ['EC2_ACCESS_KEY']
    ec2_ctx.user_key = os.environ['EC2_SECRET_KEY']

    # The SSH keypair we want to use the access the EC2 VM. If the keypair is
    # not yet registered on EC2 saga will register it automatically.  This
    # context specifies the key for VM startup, ie. the VM will be configured to
    # accept this key
    ec2keypair_ctx = rs.Context('EC2_KEYPAIR')
    ec2keypair_ctx.token    = os.environ['EC2_KEYPAIR_ID']
    ec2keypair_ctx.user_key = os.environ['EC2_KEYPAIR']
    ec2keypair_ctx.user_id  = 'root'  # the user id on the target VM

    # We specify the *same* ssh key for ssh access to the VM.  That now should
    # work if the VM go configured correctly per the 'EC2_KEYPAIR' context
    # above.
    ssh_ctx = rs.Context('SSH')
    ssh_ctx.user_id  = 'root'
    ssh_ctx.user_key = os.environ['EC2_KEYPAIR']

    session = rs.Session(False)  # FALSE: don't use other (default) contexts
    session.contexts.append(ec2_ctx)
    session.contexts.append(ec2keypair_ctx)
    session.contexts.append(ssh_ctx)

    cr  = None  # compute resource handle
    rid = None  # compute resource ID
    try:

        # ----------------------------------------------------------------------
        #
        # reconnect to VM (ID given in ARGV[1])
        #
        if len(sys.argv) > 1:
            
            rid = sys.argv[1]

            # reconnect to the given resource
            print('reconnecting to %s' % rid)
            cr = rs.resource.Compute(id=rid, session=session)
            print('reconnected  to %s' % rid)
            print("  state : %s (%s)" % (cr.state, cr.state_detail))


        # ----------------------------------------------------------------------
        #
        # start a new VM
        #
        else:

            # start a VM if needed
            # in our session, connect to the EC2 resource manager
            rm = rs.resource.Manager("ec2://aws.amazon.com/", session=session)

            # Create a resource description with an image and an OS template,.
            # We pick a small VM and a plain Ubuntu image...
            cd = rs.resource.ComputeDescription()
            cd.image    = 'ami-0256b16b'    # plain ubuntu
            cd.template = 'Small Instance'

            # Create a VM instance from that description.
            cr  = rm.acquire(cd)
            rid = cr.id

            print("\nWaiting for VM to become active...")


        # ----------------------------------------------------------------------
        #
        # use the VM
        #
        # Wait for the VM to 'boot up', i.e., become 'ACTIVE'
        cr.wait(rs.resource.ACTIVE)

        # Query some information about the newly created VM
        print("Created VM: %s"      %  cr.id)
        print("  state   : %s (%s)" % (cr.state, cr.state_detail))
        print("  access  : %s"      %  cr.access)

        # give the VM some time to start up comlpetely, otherwise the subsequent
        # job submission might end up failing...
        time.sleep(60)

        # create a job service which uses the VM's access URL (cr.access)
        js = rs.job.Service(cr.access, session=session)

        jd = rs.job.Description()
        jd.executable = '/bin/sleep'
        jd.arguments = ['30']

        job = js.create_job(jd)
        job.run()

        print("\nRunning Job: %s" % job.id)
        print("  state : %s" % job.state)
        job.wait()
        print("  state : %s" % job.state)


    except rs.SagaException as ex:
        # Catch all saga exceptions
        print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        raise


    except Exception as e:
        # Catch all other exceptions
        print("An Exception occured: %s " % e)
        raise


    finally:

        # ----------------------------------------------------------------------
        #
        # shut VM down (only when id was specified on command line)
        if cr and rid:
            cr.destroy()
            print("\nDestroyed VM: %s" % cr.id)
            print("  state : %s (%s)" % (cr.state, cr.state_detail))


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":
    
    sys.exit(main())


