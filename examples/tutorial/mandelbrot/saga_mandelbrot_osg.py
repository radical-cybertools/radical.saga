
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import sys
import time

from PIL import Image

import radica.saga as rs


REMOTE_JOB_ENDPOINT = "condor://localhost"

# the dimension (in pixel) of the whole fractal
IMGX = 2048
IMGY = 2048

# the number of tiles in X and Y direction
TILESX = 2
TILESY = 2

if __name__ == "__main__":
    try:
        # list that holds the jobs
        jobs = []

        jobservice = rs.job.Service(REMOTE_JOB_ENDPOINT)

        for x in range(0, TILESX):
            for y in range(0, TILESY):

                # describe a single Mandelbrot job. we're using the
                # directory created above as the job's working directory
                outputfile = 'tile_x%s_y%s.gif' % (x, y)
                jd = rs.job.Description()
                # candidate hosts can be changed / and or commented out
                # the list below seems to be a good working set for OSG
                #jd.candidate_hosts = ["FNAL_FERMIGRID", "cinvestav", "SPRACE",
                #                      "NYSGRID_CORNELL_NYS1", "Purdue-Steele",
                #                      "MIT_CMS_CE2", "SWT2_CPB", "AGLT2_CE_2",
                #                      "UTA_SWT2", "GridUNESP_CENTRAL",
                #                      "USCMS-FNAL-WC1-CE3"]
                # on OSG we need to stage in the data with the jobs. we
                # can't use the saga filesystem API to copy data around since
                # the execution location of the jobs is not known a priori
                jd.file_transfer     = ["mandelbrot.sh > mandelbrot.sh",
                                        "mandelbrot.py > mandelbrot.py",
                                        "%s < %s" % (outputfile, outputfile)]
                jd.wall_time_limit   = 10
                jd.total_cpu_count   = 1
                jd.executable        = '/bin/sh'
                jd.arguments         = ['mandelbrot.sh', IMGX, IMGY, 
                                        (IMGX/TILESX*x), (IMGX/TILESX*(x+1)),
                                        (IMGY/TILESY*y), (IMGY/TILESY*(y+1)),
                                        outputfile]
                # create the job from the description
                # above, launch it and add it to the list of jobs
                job = jobservice.create_job(jd)
                job.run()
                jobs.append(job)
                print(' * Submitted %s. Output will be written to: %s' % (job.id, outputfile))

        # wait for all jobs to finish
        while len(jobs) > 0:
            for job in jobs:
                jobstate = job.get_state()
                print(' * Job %s status: %s' % (job.id, jobstate))
                if jobstate in [rs.job.DONE, rs.job.FAILED]:
                    jobs.remove(job)
            time.sleep(5)

        # stitch together the final image
        fullimage = Image.new('RGB', (IMGX, IMGY), (255, 255, 255))
        print(' * Stitching together the whole fractal: mandelbrot_full.gif')
        for x in range(0, TILESX):
            for y in range(0, TILESY):
                partimage = Image.open('tile_x%s_y%s.gif' % (x, y))
                fullimage.paste(partimage,
                                (IMGX/TILESX*x, IMGY/TILESY*y,
                                 IMGX/TILESX*(x+1), IMGY/TILESY*(y+1)))
        fullimage.save("mandelbrot_full.gif", "GIF")
        sys.exit(0)

    except rs.SagaException as ex:
        # Catch all saga exceptions
        print("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Trace back the exception. That can be helpful for debugging.
        print(" \n*** Backtrace:\n %s" % ex.traceback)
        sys.exit(-1)

    except KeyboardInterrupt:
    # ctrl-c caught: try to cancel our jobs before we exit
        # the program, otherwise we'll end up with lingering jobs.
        for job in jobs:
            job.cancel()
        sys.exit(-1)
