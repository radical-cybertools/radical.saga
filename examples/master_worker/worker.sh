#activate python virtual environment
source $6/bin/activate

#pull bash arguments
for a in $* ; do
	args="$args $a "
done

#call worker.py with arguments passed from master
echo $7
for ((i=0; i<$7; i++))
do
   python worker.py$args &
done
wait