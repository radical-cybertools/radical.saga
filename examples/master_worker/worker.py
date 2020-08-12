#!/usr/bin/env python

import sys
import redis
import subprocess
import datetime


# define benchmarking variables
execution_time     = datetime.timedelta(0)
communication_time = datetime.timedelta(0)


def check_jobs(redis_host, redis_password, redis_list):
    global execution_time
    global communication_time

    now = datetime.datetime.now()

    # define redis server
    redis_server = redis.Redis(host=redis_host, port=6379,
                               password=redis_password)

    # pull tasks from Redis server
    while 1:
            now = datetime.datetime.now()
            task = redis_server.lpop(redis_list)
            communication_time += datetime.datetime.now() - now
            if (task is not None):
                now = datetime.datetime.now()
                p = subprocess.Popen([task], shell=True)
                p.wait()
                execution_time += datetime.datetime.now() - now
            else:
                break


if __name__ == "__main__":

    global execution_time
    global communication_time

    # check Redis server for jobs
    check_jobs(sys.argv[1], sys.argv[2], sys.argv[3])

    # Write benchmarking information to file
    timefile = open("benchmarks_" + str(sys.argv[4]) + ".txt", 'a')
    timefile.write("Communication Time: " + str(communication_time) + "\n")
    timefile.write("Execution Time: " + str(execution_time) + "\n\n")
    timefile.close()

    sys.exit(0)


