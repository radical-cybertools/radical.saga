
# set term pdf enhanced color "Times-Roman" 23
# set term postscript eps enhanced color "Times-Roman" 18
set term pdfcairo enhanced color font "Arial,12"

set output 'jobs_per_second.pdf'
set pointsize 2
set key       left Left
set xlabel    'number of threads' font "Times-Italic, 20"
set ylabel    'jobs per second'   font "Times-Italic, 20"
set xtic      0,1
set ytic      0,20
set mxtics    1
set mytics    4
plot[0:20][0:110] \
    './benchmark.fork.localhost.dat'               using 3:10   title 'LOC (fork,    localhost)' with linespoints ps 1 pt 1 lt 1 lc 1 lw 3, \
    './benchmark.ssh.localhost.dat'                using 3:10   title 'LOC (ssh,     localhost)' with linespoints ps 1 pt 6 lt 1 lc 1 lw 3, \
    './benchmark.ssh.boskop.dat'                   using 3:10   title 'LAN (ssh,     boskop)'    with linespoints ps 1 pt 6 lt 1 lc 2 lw 3, \
    './benchmark.ssh.silver.dat'                   using 3:10   title 'LAN (ssh,     silver)'    with linespoints ps 1 pt 6 lt 1 lc 2 lw 3, \
    './benchmark.ssh.india.futuregrid.org.dat'     using 3:10   title 'WAN (ssh,     india)'     with linespoints ps 1 pt 6 lt 1 lc 3 lw 3, \
    './benchmark.pbs+ssh.india.futuregrid.org.dat' using 3:10   title 'WAN (pbs+ssh, india)'     with linespoints ps 1 pt 7 lt 1 lc 3 lw 3

