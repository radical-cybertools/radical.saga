#!/usr/bin/env python

__author__    = "Andre Merzky, Matteo Turilli, Ole Weidner"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


import os
import sys
import time

import radical.saga as rs


"""  
This is an example which shows how to access Amazon EC2 clouds via the SAGA
resource package. The code expects the environment variables EC2_ACCESS_KEY and
EC2_SECRET_KEY to contain the respective authentication tokens required for EC2
access.  It also expects EC2_KEYPAIR to point to the ssh key to be used in the
EC2 keypair authentication.

This program has different modes of operation:

  * *Help* ::
  
    # python examples/resource/ec2.py -h
    
    Usage:

        %s -l             :  list    VMs
        %s -c <id> [...]  :  create  VM
        %s -u <id> [...]  :  use     VMs (run jobs)
        %s -d <id> [...]  :  destroy VMs

    Environment:
       
       EC2_URL        : backend manager service endpoint
       EC2_ACCESS_KEY : id  for backend access
       EC2_SECRET_KEY : key for backend access
       EC2_KEYPAIR_ID : name of keypair for VM access
       EC2_KEYPAIR    : public ssh key for VM access


  * *Listing* of templates and existing VMs on EC2::

    # python examples/resource/ec2.py -l
    
    compute resources
      [ec2://aws.amazon.com/]-[i-cba515ab]
      [ec2://aws.amazon.com/]-[i-f93f2299]
    
    compute templates
      Micro Instance
      Small Instance
      Medium Instance
      Large Instance
      Extra Large Instance
      High-Memory Extra Large Instance
      High-Memory Double Extra Large Instance
      High-Memory Quadruple Extra Large Instance
      Extra Large Instance
      Double Extra Large Instance
      High-CPU Medium Instance
      High-CPU Extra Large Instance
      Cluster Compute Quadruple Extra Large Instance
      Cluster Compute Eight Extra Large Instance
      Cluster GPU Quadruple Extra Large Instance
      High Memory Cluster Eight Extra Large
      High Storage Eight Extra Large Instance
    

  * *Creating* a VM instance on EC2::

    # python examples/resource/ec2.py -c

    Created VM
      id           : 
      state        : PENDING (pending)
      access       : None


  * *Using* a VM instance on EC2::
  
    # python examples/resource/ec2.py -u '[ec2://aws.amazon.com/]-[i-e0d2ad8a]'

    connecting to [ec2://aws.amazon.com/]-[i-e0d2ad8a]
      id           : [ec2://aws.amazon.com/]-[i-e0d2ad8a]
      state        : PENDING (pending)
      wait for ACTIVE state
      state        : ACTIVE (running)
    running job
      job state    : Running
      job state    : Done


  * *Destroying* a VMs instance on EC2::

    # python examples/resource/ec2.py -d '[ec2://aws.amazon.com/]-[i-e0d2ad8a]'

    reconnecting to id [ec2://aws.amazon.com/]-[i-e0d2ad8a]
      id           : [ec2://aws.amazon.com/]-[i-e0d2ad8a]
      state        : ACTIVE (running)
      access       : ssh://107.21.154.248/
    
    shutting down [ec2://aws.amazon.com/]-[i-e0d2ad8a] (ACTIVE)
      state        : EXPIRED (destroyed by user)
    
"""


# ------------------------------------------------------------------------------
#
# helper
#
def usage (msg = None) :

    if  msg :
        print("\n    Error: %s\n"  %  msg)

    print("""
    Usage:

        %s -l             :  list    VMs
        %s -c <id> [...]  :  create  VM
        %s -u <id> [...]  :  use     VMs (run jobs)
        %s -d <id> [...]  :  destroy VMs


    Environment:
       
       EC2_URL        : backend manager service endpoint
       EC2_ACCESS_KEY : id  for backend access
       EC2_SECRET_KEY : key for backend access
       EC2_KEYPAIR_ID : name of keypair for VM access
       EC2_KEYPAIR    : public ssh key for VM access


    """)

    if msg : sys.exit (-1)
    else   : sys.exit ( 0)


# ------------------------------------------------------------------------------
#
def state2str (state) :

    if state == rs.resource.UNKNOWN  : return "UNKNOWN"
    if state == rs.resource.NEW      : return "NEW   "
    if state == rs.resource.PENDING  : return "PENDING"
    if state == rs.resource.ACTIVE   : return "ACTIVE"
    if state == rs.resource.CANCELED : return "CANCELED"
    if state == rs.resource.EXPIRED  : return "EXPIRED"
    if state == rs.resource.DONE     : return "DONE  "
    if state == rs.resource.FAILED   : return "FAILED"



# ------------------------------------------------------------------------------
#
# set up the connection to EC2
#

if not 'EC2_URL'        in os.environ : usage ("no %s in environment" % 'EC2_URL'       )
if not 'EC2_ACCESS_KEY' in os.environ : usage ("no %s in environment" % 'EC2_ACCESS_KEY')
if not 'EC2_SECRET_KEY' in os.environ : usage ("no %s in environment" % 'EC2_SECRET_KEY')
if not 'EC2_KEYPAIR_ID' in os.environ : usage ("no %s in environment" % 'EC2_KEYPAIR_ID')
if not 'EC2_KEYPAIR'    in os.environ : usage ("no %s in environment" % 'EC2_KEYPAIR'   )

server = rs.Url(os.environ['EC2_URL'])


# in order to connect to EC2, we need an EC2 ID and KEY
c1 = rs.Context ('ec2')
c1.user_id  = os.environ['EC2_ACCESS_KEY']
c1.user_key = os.environ['EC2_SECRET_KEY']
c1.server   = server

# in order to access a created VM, we additionally need to point to the ssh
# key which is used for EC2 VM contextualization, i.e. as EC2 'keypair'.
# If the keypair is not yet registered on EC2, it will be registered by SAGA
# -- but then a user_key *must* be specified (only the public key is ever
# transfererd to EC2).
c2 = rs.Context ('ec2_keypair')
c2.token     = os.environ['EC2_KEYPAIR_ID']
c2.user_cert = os.environ['EC2_KEYPAIR']
c2.user_id   = 'ubuntu'         # the user id on the target VM
c2.server    = server

# we create a session for all SAGA interactions, and attach the respective
# security contexts.  Those are now avail for all SAGA objects created in
# that session
s = rs.Session (False)  # FALSE: don't use any other (default) contexts
s.contexts.append (c1)
s.contexts.append (c2)

# in this session, connect to the EC2 resource manager
rm  = rs.resource.Manager (server, session=s)


# --------------------------------------------------------------------------
# 
# setup is done, evaluate command line parameters
#
args = sys.argv[1:]

# --------------------------------------------------------------------------
if  '-l' in args :

    args.remove ('-l')
    if  len (args) > 0 :
        usage ("no additional args allowed on '-l'")

    # list known VMs (compute resources)
    print("\ncompute resources")
    for cr_id in rm.list () :
        print("  %s" % cr_id)

    # list the available VM templates
    print("\ncompute templates")
    for tmp in rm.list_templates () :
        print("  %s" % tmp)

    # we can also list the available OS images, as per below -- but since
    # the list of OS images avaialble on EC2 is *huge*, this operation is
    # rather slow (libcloud does one additional hop per image, for
    # inspection)

    # {'name': 'None (cube-1-0-5-2012-09-07)', 'ispublic': 'true', 'state': 'available', 'rootdevicetype': 'instance-store', 'imagetype': 'machine'}
    print("\nOS images")
    
    descr = None
    ispublic = None

    for osi in rm.list_images () :
        descr = rm.get_image (osi)

        if descr['ispublic'] == 'true' :
            ispublic = 'public'
        else:
            ispublic = 'private'

        print("  %s - %s, %s, %s" % (osi, descr['name'], ispublic,
                                     descr['state']))

    print()
    sys.exit (0)


# --------------------------------------------------------------------------
elif  '-c' in args :

    args.remove ('-c')
    if  len (args) == 0 :
        usage ("additional args required on '-c'")

    for image in args :

        print("\ncreating an instance from image %s" % image)

        # create a resource description with an image and an OS template, out of
        # the ones listed above.  We pick a small VM and a plain Ubuntu image...
        cd = rs.resource.ComputeDescription ()
        cd.image    = image
        cd.template = 'Small Instance'

        # create a VM instance with that description, and inspect it for some
        # detailes
        cr = rm.acquire (cd)
        print("\nCreated VM")
        print("  id           : %s"       %  cr.id)
        print("  state        : %s (%s)"  %  (state2str(cr.state), cr.state_detail))
        print("  access       : %s"       %  cr.access)

    sys.exit (0)

# --------------------------------------------------------------------------
elif  '-u' in args :

    args.remove ('-u')
    if  len (args) == 0 :
        usage ("additional args required on '-u'")

    # we want to reconnect to running VMs, specified by their IDs
    for vm_id in args :

        print("\nconnecting to %s" % vm_id)

        # get a handle on that VM, and print some information
        cr = rm.acquire (vm_id)

        print("  id           : %s"       %  cr.id)
        print("  state        : %s (%s)"  %  (state2str(cr.state), cr.state_detail))

        # make sure the machine is not in final state already
        if  cr.state in [rs.resource.EXPIRED,
                         rs.resource.DONE,
                         rs.resource.FAILED] :
            print("  VM %s is alrady in final state"  %  vm_id)
            continue

        # we only can run jobs on ACTIVE machines -- so lets wait until the VM
        # is in that state.
        # Note: the careful coder will spot the subtle race condition between the 
        # check above and the check on this line... ;-)
        if cr.state != rs.resource.ACTIVE :
          print("  wait for ACTIVE state")
          cr.wait (rs.resource.ACTIVE)

          # Once a VM comes active, it still needs to boot and setup the ssh
          # daemon to be usable -- we thus wait for a while
          time.sleep (60)

        print("  state        : %s (%s)"  %  (state2str(cr.state), cr.state_detail))
        print("  access       : %s"       %   cr.access)


        # The session created above contains the ssh context to access the VM
        # instance -- that context was created from the ec2_keypair context
        # which was earlier used for VM contextualization.  So we use that
        # session to create a job service instance for that VM:
        js = rs.job.Service (cr.access, session=s)

        print("running job")
        # all ready: do the deed!
        j = js.run_job ('sleep 10')
        print("  job state    : %s"  %  j.state)
        j.wait ()
        print("  job state    : %s"  %  j.state)

    print()
    sys.exit (0)



# --------------------------------------------------------------------------
elif  '-d' in args :

    args.remove ('-d')
    if  len (args) == 0 :
        usage ("additional args required on '-d'")

    # we want to reconnect to running VMs, specified by their IDs
    for vm_id in args :

        print("\nreconnecting to id %s" % vm_id)

        # get a handle on that VM, and print some information
        cr = rm.acquire (vm_id)

        print("  id           : %s"       %  cr.id)
        print("  state        : %s (%s)"  %  (state2str(cr.state), cr.state_detail))
        print("  access       : %s"       %  cr.access)

        if cr.state in [rs.resource.EXPIRED,
                        rs.resource.DONE,
                        rs.resource.FAILED] :
            print("  VM %s is alrady in final state"  %  vm_id)
            continue

        print("\nshutting down  %s "      %  cr.id)
        cr.destroy ()
        print("  state        : %s (%s)"  %  (state2str(cr.state), cr.state_detail))
    
    print() 
    sys.exit (0)


# --------------------------------------------------------------------------
else :

    usage ('invalid arguments')

