#!/bin/bash

N=1000
TIMEFORMAT="%3Rs   %3Us   %3Ss   %P%%"

echo "====================================================================="
echo "run $N jobs"
echo "====================================================================="

echo
echo "====================================================================="
echo "/bin/sleep 1 &"
echo "---------------------------------------------------------------------"
time (for i in $(seq 1 $N); do
  /bin/sleep 1 &
done) > /dev/null
echo "====================================================================="

echo
echo "====================================================================="
echo "RUN sleep 1" 
echo "---------------------------------------------------------------------"
time (for i in $(seq 1 $N); do
  echo "RUN sleep 1" 
done | sh ~/.saga/adaptors/ssh_job/wrapper.sh) > /dev/null
echo "====================================================================="

echo
echo "====================================================================="
echo "RUN sleep 1 @   cyder.cct.lsu.edu" 
echo "---------------------------------------------------------------------"
time ((for i in $(seq 1 $N); do
  echo "RUN sleep 1" 
done ; echo "QUIT") \
   | ssh amerzky@cyder.cct.lsu.edu "sh ~/.saga/adaptors/ssh_job/wrapper.sh")
echo "====================================================================="

echo
echo "====================================================================="
echo "js = saga.job.Service ('fork://localhost/')"
echo "j  = js.create_job ({executable:'/bin/data', arguments:['1']})"
echo "---------------------------------------------------------------------"
time python -c "
import saga
jd = saga.job.Description ()
jd.executable = '/bin/sleep'
jd.arguments  = ['1']
js = saga.job.Service ('fork://localhost/')
for i in range (0, $N) :
  j=js.create_job (jd)
  j.run ()
"
echo "====================================================================="

echo
echo "====================================================================="
echo "js = saga.job.Service ('fork://localhost/')"
echo "j  = js.run_job (\"/bin/sleep 1\")"
echo "---------------------------------------------------------------------"
time python -c "
import saga
js = saga.job.Service ('fork://localhost/')
for i in range (0, $N) :
  j=js.run_job ('/bin/sleep 1')
"
echo "====================================================================="

echo
echo "====================================================================="
echo "js = saga.job.Service ('ssh://localhost/')"
echo "j  = js.create_job ({executable:'/bin/data', arguments:['1']})"
echo "---------------------------------------------------------------------"
time python -c "
import saga
jd = saga.job.Description ()
jd.executable = '/bin/sleep'
jd.arguments  = ['1']
js = saga.job.Service ('ssh://localhost/')
for i in range (0, $N) :
  j=js.create_job (jd)
  j.run ()
"
echo "====================================================================="

echo
echo "====================================================================="
echo "js = saga.job.Service ('ssh://amerzky@cyder.cct.lsu.edu/')"
echo "j  = js.create_job ({executable:'/bin/data', arguments:['1']})"
echo "---------------------------------------------------------------------"
time python -c "
import saga
jd = saga.job.Description ()
jd.executable = '/bin/sleep'
jd.arguments  = ['1']
js = saga.job.Service ('ssh://amerzky@cyder.cct.lsu.edu/')
for i in range (0, $N) :
  j=js.create_job (jd)
  j.run ()
"
echo "====================================================================="

echo
echo "====================================================================="
echo "js = saga.job.Service ('gsissh://trestles-login.sdsc.edu/')"
echo "j  = js.create_job ({executable:'/bin/data', arguments:['1']})"
echo "---------------------------------------------------------------------"
time python -c "
import saga
jd = saga.job.Description ()
jd.executable = '/bin/sleep'
jd.arguments  = ['1']
js = saga.job.Service ('gsissh://trestles-login.sdsc.edu/')
for i in range (0, $N) :
  j=js.create_job (jd)
  j.run ()
"
echo "====================================================================="

