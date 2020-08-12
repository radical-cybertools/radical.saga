#!/usr/bin/env python

import os
import sys
import redis
import datetime

import radical.saga as rs


coordination_time = datetime.timedelta(0)
communication_time = datetime.timedelta(0)

jobs = []
redis_list = "vshah505_redis_list"
master_sftp = "vishal.shah505@repex1.tacc.utexas.edu" + os.getcwd() + "/"


def initiate_workers(machine_parameters, number_of_workers, number_of_machines):

    workers = int(number_of_workers) / int(number_of_machines)
    try:
        x = 0
        for parameters in machine_parameters:
            # create a job service
            js = rs.job.Service(parameters[1])

            # describe our job
            jd = rs.job.Description()
            jd.total_cpu_count   = workers
            jd.working_directory = parameters[2]
            jd.executable        = "/bin/bash"
            jd.arguments         = ['worker.sh', parameters[3], parameters[4],
                                    redis_list, "worker" + str(x), master_sftp,
                                    parameters[5], workers]
            jd.error             = "master_err.stderr"

            # create the job (state:New)
            job = js.create_job(jd)
            job.run()
            jobs.append(job)

    except rs.SagaException as ex:
        print("An error occured during job execution: %s" % (str(ex)))
        sys.exit(-1)


def read_machine_information(filename):
    parameters = []
    description = []
    f = open(filename, "r")
    for line in f:
        if(line[0] == '# '):
            description.append(line[1:len(line) - 1])
        elif(line == "\n"):
            parameters.append(list(description))
            del description[:]
        else:
            description.append(line[(line.find(':') + 2):len(line) - 1])

    return parameters


def read_dependencies(filename):
    dependencies = []
    description = []
    f = open(filename, "r")
    for line in f:
        if(line[0] == '# '):
            description.append(line[1:len(line) - 1])
        elif(line == "\n"):
            dependencies.append(list(description))
            del description[:]
        else:
            description.append(line[:len(line) - 1])
    return dependencies


def process_dependencies(dependencies, machine_parameters):
    i = 0
    for x in dependencies:

        ctx = rs.Context("ssh")
        ctx.user_id = "vshah505"

        session = rs.Session()
        session.add_context(ctx)

        flag = 1
        for files in x:
            if flag == 1:
                service = machine_parameters[i][1]
                service = service[(service.find('/') + 2):]
                flag = 0
            else:
                # print("file://localhost" + os.getcwd() + "/" + files)
                # print("file://"+service+machine_parameters[i][2])
                f = rs.filesystem.File("file://localhost" + os.getcwd() + "/" +
                                                         files, session=session)
                f.copy("sftp://" + service + machine_parameters[i][2] + files)
                print("Successful copy")

        i += 1


def write_tasks_to_redis(redis_server, tasks):

    for t in tasks:
        redis_server.push(redis_list, tasks)


def main(number_of_tasks, number_of_workers, number_of_machines):
    global coordination_time 
    global communication_time

    # open file to record times
    timefile = open("benchmarks.txt", 'a')

    # define redis server
    redis_server = redis.Redis(host="gw68.quarry.iu.teragrid.org", port=6379,
                               password="ILikeBigJob_wITH-REdIS")    

    # pull server parameters from file
    machine_parameters = read_machine_information("server_information.txt")

    # pull dependencies from file
    dependencies = read_dependencies("dependencies.txt")

    # transfer dependencies to machines
    now = datetime.datetime.now()
    process_dependencies(dependencies, machine_parameters)
    communication_time += datetime.datetime.now() - now

    # write task list to redis server
    now = datetime.datetime.now()
    for i in range(0, int(number_of_tasks)):
        redis_server.rpush(redis_list,"sleep 1")
    communication_time += datetime.datetime.now() - now

    # create workers
    now = datetime.datetime.now()
    initiate_workers(machine_parameters, number_of_workers, number_of_machines)
    coordination_time += datetime.datetime.now() - now

    # check job status
    while (len(jobs) > 0):
        for job in jobs:
            jobstate = job.get_state()
            print(' * Job %s status: %s' % (job.id, jobstate))
            if str(jobstate) is "Done":
                jobs.remove(job)

    timefile.write("Coordination Time : " + str(coordination_time) + "\n")
    timefile.write("Communication Time: " + str(communication_time) + "\n")

    timefile.close()


if __name__ == "__main__":

    main(sys.argv[1], sys.argv[2], sys.argv[3])


