
import radical.saga as saga

for i in range (100) :
    f = saga.filesystem.File ("ssh://localhost/etc/passwd")
    f.copy ("/tmp/passwd_%d" % i)
    f.close ()

