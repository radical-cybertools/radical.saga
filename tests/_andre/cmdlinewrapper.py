
from saga.utils.cmdlinewrapper import CommandLineWrapper

clw = CommandLineWrapper.init_as_subprocess_wrapper()
clw.open()
print clw.run_sync('/bin/date', ['-r 2'])
clw.close()

