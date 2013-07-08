
# set term pdf enhanced color "Times-Roman" 23
# set term postscript eps enhanced color "Times-Roman" 18
set term pdfcairo enhanced color font "Arial,12"

set output 'jobs_per_second.pdf'
set pointsize 2
set key       right Left
set xlabel    'number of threads'      font "Times-Italic, 20"
set ylabel    'submit 1024 jobs (s)'   font "Times-Italic, 20"
set xtic      0,1
set ytic      0,100
set mxtics    1
set mytics    4
plot[1:20][0:500] \
    './results/benchmark.job_run.fork.localhost.dat'          using 3:(($6)*1024/10000) title 'LOC (fork,    localhost)'       with linespoints ps 1 pt 1 lt 1 lc 1 lw 3, \
    './results/benchmark.job_run.fork.india.dat'              using 3:6                 title 'LOC (fork,    india)'           with linespoints ps 1 pt 1 lt 1 lc 3 lw 3, \
    './results/benchmark.job_run.ssh.localhost.dat'           using 3:(($6)*1024/10000) title 'LOC (ssh,     localhost)'       with linespoints ps 1 pt 6 lt 1 lc 1 lw 3, \
    './results/benchmark.job_run.ssh.boskop.dat'              using 3:(($6)*1024/10000) title 'LAN (ssh,     boskop)'          with linespoints ps 1 pt 6 lt 1 lc 2 lw 3, \
    './results/benchmark.job_run.ssh.silver.dat'              using 3:(($6)*1024/10000) title 'LAN (ssh,     silver)'          with linespoints ps 1 pt 6 lt 1 lc 2 lw 3, \
    './results/benchmark.job_run.ssh.india.dat'               using 3:(($6)*1024/10000) title 'WAN (ssh,     india)'           with linespoints ps 1 pt 6 lt 1 lc 3 lw 3, \
    './results/benchmark.job_run.pbs+ssh.india.dat'           using 3:(($6)*1024/10000) title 'WAN (pbs+ssh, india)'           with linespoints ps 1 pt 7 lt 1 lc 3 lw 3, \
    './results/benchmark.job_run_bulk_threaded.ssh.india.dat' using 3:6                 title 'WAN (ssh,     india, bulk[32])' with linespoints ps 1 pt 8 lt 1 lc 3 lw 3

set output 'jobs_over_bulks.pdf'
set pointsize 2
set key       right Left
set xlabel    'size of bulks'          font "Times-Italic, 20"
set ylabel    'submit 1024 jobs (s)'   font "Times-Italic, 20"
set xtic      0,2
set ytic      0,100
set mxtics    1
set mytics    4
set logscale  x
plot[1:1024][0:500] \
    './results/benchmark.job_run_bulk.fork.localhost.dat'     using 4:6 title 'LOC (fork,    localhost)'       with linespoints ps 1 pt 1 lt 1 lc 1 lw 3, \
    './results/benchmark.job_run_bulk.ssh.localhost.dat'      using 4:6 title 'LOC (ssh,     localhost)'       with linespoints ps 1 pt 6 lt 1 lc 1 lw 3, \
    './results/benchmark.job_run_bulk.ssh.silver.dat'         using 4:6 title 'LAN (ssh,     silver)'          with linespoints ps 1 pt 6 lt 1 lc 2 lw 3, \
    './results/benchmark.job_run_bulk.ssh.india.dat'          using 4:6 title 'WAN (ssh,     india)'           with linespoints ps 1 pt 6 lt 1 lc 3 lw 3

