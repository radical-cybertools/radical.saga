
import saga

for i in range (100) :
    f = saga.filesystem.File ("ssh://tg803521@stampede.tacc.utexas.edu/etc/passwd")
    f.copy ("/tmp/passwd_%d" % i)
    f.close ()

