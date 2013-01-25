
from saga.utils.cmdlinewrapper import CommandLineWrapper

clw = CommandLineWrapper(scheme="fork")
clw.open()
print clw.run_sync('/bin/date', [])
clw.close()

