
import os
import sys
import saga
import time
import pprint

from pudb import set_interrupt_handler; set_interrupt_handler()

# ------------------------------------------------------------------------------
#
# defines
#
START = 'start'
STOP  = 'stop'


# ------------------------------------------------------------------------------
#
# helper
#
def usage (msg = None) :

    if  msg :
        print "\n    Error: %s\n"  %  msg

    print """
    Usage:

        %s -s              :  start a new VM
        %s -d <id> [...]   :  destroy specified VMs

    """

    if msg : sys.exit (-1)
    else   : sys.exit ( 0)


def state2str (state) :

    if state == saga.resource.UNKNOWN  : return "UNKNOWN"
    if state == saga.resource.NEW      : return "NEW   "
    if state == saga.resource.PENDING  : return "PENDING"
    if state == saga.resource.ACTIVE   : return "ACTIVE"
    if state == saga.resource.CANCELED : return "CANCELED"
    if state == saga.resource.EXPIRED  : return "EXPIRED"
    if state == saga.resource.DONE     : return "DONE  "
    if state == saga.resource.FAILED   : return "FAILED"



# ------------------------------------------------------------------------------
#
def main () :
    """
    scan argv
    if  -s in argv:
        create VM instance
        run job on VM
        exit
    if  -d in argv
        for each vm_id in argv
            connect to vm with id vm_id
            run job
            destroy vm instance
        exit
    """

    mode   = None
    vm_ids = []
    args   = sys.argv[1:]

    if  '-s' in args :
        mode = START
        args.remove ('-s')
        if  len (args) > 0 :
            usage ("no additional args allowed on '-s'")


    if  '-d' in args :
        mode = STOP
        args.remove ('-d')
        if  len (args) == 0 :
            usage ("additional args required on '-d'")
        # we may have VM IDs to connect to
        vm_ids = args

    # make sure we know what to do
    if  not mode :
        usage ()



    # in order to connect to EC2, we need an EC2 ID and KEY
    c1 = saga.Context ('ec2')
    c1.user_id  = os.environ['EC2_ID']
    c1.user_key = os.environ['EC2_KEY']

    # in order to access a created VM, we additionally need to point to the ssh
    # key which is used for EC2 VM contextualization, i.e. as ec2_keypair.
    c2 = saga.Context ('ec2_keypair')
    c2.token    = 'futuregrid'
    c2.user_key = '/home/merzky/.ssh/id_rsa_futuregrid.pub'
    c2.user_id  = 'root'  # the user id on the target VM

    # NOTE: the above ec2_keypair context should spawn an ssh context, alas this
    # is not possible with the present libcloud (for details, see comments in
    # ec2 adaptor).  We thus add that ssh context manually
    c3 = saga.Context ('ssh')
    c3.user_id   = 'root'
    c3.user_cert = '/home/merzky/.ssh/id_rsa_futuregrid'


    # we create a session for all SAGA interactions, and attach the respective
    # security contexts.  Those are now avail for all SAGA objects created in
    # that session
    s = saga.Session (False)  # FALSE: don't use any other (default) contexts
    s.contexts.append (c1)
    s.contexts.append (c2)
    s.contexts.append (c3)

  # print "\nContexts: "
  # pprint.pprint (s.contexts)

    # in this session, connect to the EC2 resource manager
    rm  = saga.resource.Manager ("ec2://aws.amazon.com/", session=s)
    crs = [] # list of compute resources


    if  mode == START :

        # we want to start a VM -- list the available VM templates
        print "\navailable compute templates"
        print rm.list_templates ()

        # we can also list the available OS images, as per below -- but since
        # the list of OS images avaialble on EC2 is *huge*, this operation is
        # rather slow (libcloud does one additional hop per image, for
        # inspection)
        # print "\navailable OS images"
        # print rm.list_images ()

        # create a resource description with an image and an OS template, out of
        # the ones listed above.  We pick a small VM and a plain Ubuntu image...
        cd = saga.resource.ComputeDescription ()
        cd.image    = 'ami-0256b16b'
        cd.template = 'Small Instance'

        # create a VM instance with that description, and inspect it for some
        # detailes
        cr = rm.acquire (cd)
        print "\nCreated VM"
        print "  id           : %s"       %  cr.id
        print "  state        : %s (%s)"  %  (state2str(cr.state), cr.state_detail)
        print "  access       : %s"       %  cr.access

        # keep that instance in our list of resources to run jobs on
        crs.append (cr)


    else :  # mode == STOP

        # we want to reconnect to running VMs, specified by their IDs
        for vm_id in vm_ids :

            print "\nreconnecting to id %s" % vm_id

            # get a handle on that VM, and print some information
            cr = rm.acquire (vm_id)

            print "  id           : %s"       %  cr.id
            print "  state        : %s (%s)"  %  (state2str(cr.state), cr.state_detail)
            print "  access       : %s"       %  cr.access

            # keep that instance in our list of resources to run jobs on
            crs.append (cr)


    # in both, the running and destroy mode, we run a simple job on each compute
    # resource (VM) in our list
    for cr in crs :

        print "\nConnecting to VM %s" % cr.id

        # make sure the machine is not in final state already
        state = cr.state
        print "  state        : %s (%s)"  %  (state2str(cr.state), cr.state_detail)
        if  state in [saga.resource.EXPIRED,
                      saga.resource.DONE,
                      saga.resource.FAILED] :
            print "  VM %s is alrady in final state"  %  vm_id
            continue

        # we only can run jobs on ACTIVE machines -- so lets wait until the VM
        # is in that state.
        # Note: the careful coder will spot the subtle race condition between the 
        # check above and the check on this line... ;-)
        if state != saga.resource.ACTIVE :
          cr.wait (saga.resource.ACTIVE)

          # Once a VM comes active, it still needs to boot and setup the ssh
          # daemon to be usable -- we thus wait for a while
          # Note: this is a workaround and needs to be fixed in the adaptor!
          time.sleep (60)

        print "  state        : %s (%s)"  %  (state2str(cr.state), cr.state_detail)


        # The session created above contains the ssh context to access the VM
        # instance -- that context was created from the ec2_keypair context
        # which was earlier used for VM contextualization.  So we use that
        # session to create a job service instance for that VM:
        js = saga.job.Service (cr.access, session=s)

        print "Running job"
        # all ready: do the deed!
        j = js.run_job ('sleep 10')
        print "  job state    : %s"  %  j.state
        j.wait ()
        print "  job state    : %s"  %  j.state


    # after we run the job(s), we are done -- for the START case.  In the STOP
    # case, we now shut down the VM instances
    if mode == STOP :
        for cr in crs :
            print "\nshutting down %s (%s)" % (cr.id, state2str(cr.state))
            cr.destroy ()
            print "  state        : %s (%s)"  %  (state2str(cr.state), cr.state_detail)



# ------------------------------------------------------------------------------
#
main ()

