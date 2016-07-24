
from __future__ import absolute_import
from __future__ import print_function
import sys
import saga
import time
import multiprocessing as mp
from six.moves import range

target  = "ssh://india.futuregrid.org/"
threads = 4

def out (char) :
    sys.stdout.write (char)
    sys.stdout.flush ()


def worker_jobs () :

    time.sleep (1)
    out ('.')

    for x in range (100) :
        js = saga.job.Service (target)
        out ('J')

        for y in range (10) :
            j = js.run_job ("sleep 1")
            out ('j')

        time.sleep (2)
        js.close ()


def worker_files () :

    time.sleep (1)
    out ('.')

    for x in range (100) :
        d = saga.filesystem.Directory (target)
        out ('F')

        for y in range (10) :
            f = d.open ("etc/passwd")
            f.copy ("file://localhost/tmp/%04d_%04d_passwd.txt" % (x, y))
            f.close ()
            out ('f')

        d.close ()


procs = list()
for x in range (threads) :
    p = mp.Process (target=worker_jobs)
    p.start()
    procs.append (p)
    print('started job  worker %s' % p)

    p = mp.Process (target=worker_files)
    p.start()
    procs.append (p)
    print('started file worker %s' % p)


for p in procs :
    p.join ()
    print('joined worker %s' % p)




