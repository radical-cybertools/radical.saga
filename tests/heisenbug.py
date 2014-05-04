
import os
import sys
import time
import logging
import uuid
import saga
import pilot
import traceback
 
#------------------------------------------------------------------------------
# Redis password and eThread secret key details aquired from the environment
COORD = os.environ.get('COORDINATION_URL')
ACCESS_KEY_ID = os.environ.get('ETHREAD_ACCESS_KEY_ID')
SECRET_ACCESS_KEY= os.environ.get('ETHREAD_SECRET_ACCESS_KEY')
#------------------------------------------------------------------------------
# The coordination server
#COORD = "redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"
# The host (+username) to run BigJob on
EC2url = "aws.amazon.com"
S3url = ""
# The queue on the remote system
#QUEUE = "normal"
# The working directory on the remote cluster / machine
#WORKDIR = "/home1/02554/sagatut/XSEDETutorial/%s/example1" % USER_NAME
WORKDIR = "/home/ubuntu/NY/"
SSHKEYFILE = "/home/merzky/.ssh/id_rsa_futuregrid"
#WORKDIR2 = "/home/anjani/bigjob_test/test_saga-bigjob/agent/SE_2"
# The number of jobs you want to run
NUMBER_JOBS = 1
 
AMI    = "ami-d63572bf"
VM     = "t1.micro"
KEY    = "fg_andre"
USER   = "ubuntu"
REGION = "us-east-1a"

 
#------------------------------------------------------------------------------
#
def main():
    pilotjob1              = None
    pilot_compute_service1 = None
    pilotjob2              = None
    pilot_compute_service2 = None

    try:
        # this describes the parameters and requirements for our pilot job1
        pilot_compute_description_amazon_west1 = pilot.PilotComputeDescription()
        pilot_compute_description_amazon_west1 = {
                             "service_url": 'ec2+ssh://%s' % EC2url,
                             "number_of_processes": 2,
                             "vm_id":AMI,
                             "vm_ssh_username": USER,
                             "vm_ssh_keyname": KEY,
                             "vm_ssh_keyfile": SSHKEYFILE,
                             "vm_type":VM,
                             "region" :REGION,
                             "access_key_id":ACCESS_KEY_ID,
                             "secret_access_key":SECRET_ACCESS_KEY,
                            # "affinity_machine_label": ""
                            }
 
        # create a new pilot job1
        pilot_compute_service1 = pilot.PilotComputeService(COORD)
        pilotjob1 = pilot_compute_service1.create_pilot(pilot_compute_description_amazon_west1)
 
        # this describes the parameters and requirements for our pilot job2
        pilot_compute_description_amazon_west2 = pilot.PilotComputeDescription()
        pilot_compute_description_amazon_west2 = {
                             "service_url": 'ec2+ssh://%s' % EC2url,
                             "number_of_processes": 2,
                             "vm_id": AMI,
                             "vm_ssh_username": USER,
                             "vm_ssh_keyname": KEY,
                             "vm_ssh_keyfile": SSHKEYFILE,
                             "vm_type": VM,
                             "region" : REGION,
                             "access_key_id":ACCESS_KEY_ID,
                             "secret_access_key":SECRET_ACCESS_KEY,
                            # "affinity_machine_label": ""
                            }
 
        # create a new pilot job2
        pilot_compute_service2 = pilot.PilotComputeService(COORD)
        pilotjob2 = pilot_compute_service2.create_pilot(pilot_compute_description_amazon_west2)
 
        # submit tasks1 to pilot job1
        tasks1 = list()
        for i in range(NUMBER_JOBS):
            task_desc1 = pilot.ComputeUnitDescription()
            task_desc1.working_directory = WORKDIR
            task_desc1.executable = '/bin/echo'
            task_desc1.arguments = ['I am task number $TASK_NO from pj1', ]
            task_desc1.environment = {'TASK_NO': i}
            task_desc1.number_of_processes = 1
            task_desc1.output = 'stdout1.txt'
            task_desc1.error = 'stderr1.txt'
 
            task1 = pilotjob1.submit_compute_unit(task_desc1)
            print "* Submitted task '%s' with id '%s' to %s" % (i, task1.get_id(), EC2url)
            tasks1.append(task1)
 
        print "Waiting for tasks to finish..."
        pilotjob1.wait()
 
        # submit tasks2 to pilot job2
        tasks2 = list()
        for i in range(NUMBER_JOBS):
            task_desc2 = pilot.ComputeUnitDescription()
            task_desc2.working_directory = WORKDIR
            task_desc2.executable = '/bin/echo'
            task_desc2.arguments = ['I am task number $TASK_NO from pj2', ]
            task_desc2.environment = {'TASK_NO': i}
            task_desc2.number_of_processes = 1
            task_desc2.output = 'stdout2.txt'
            task_desc2.error = 'stderr2.txt'
 
            task2 = pilotjob2.submit_compute_unit(task_desc2)
            print "* Submitted task '%s' with id '%s' to %s" % (i, task2.get_id(), EC2url)
            tasks2.append(task2)
 
        print "Waiting for tasks to finish..."
        pilotjob2.wait()
        
        return(0)
 
    except Exception, ex:
            print "AN ERROR OCCURED: %s" % ((str(ex)))
            # print a stack trace in case of an exception -
            # this can be helpful for debugging the problem
            traceback.print_exc()
            return(-1)
 
    finally:
        # alway try to shut down pilots, otherwise jobs might end up
        # lingering in the queue
        print ("Terminating BigJob...")
        if pilotjob1 :
          pilotjob1.cancel()

        if pilot_compute_service1 :
          pilot_compute_service1.cancel()

        if pilotjob2 :
          pilotjob2.cancel()

        if pilot_compute_service2 :
          pilot_compute_service2.cancel()
 
 
if __name__ == "__main__":
    sys.exit(main())

