
from saga.utils.cmdlinewrapper import CommandLineWrapper

clw = CommandLineWrapper(scheme="ssh", host='localhost')

clw.open()

print clw.run_sync('/bin/date')
print clw.run_sync('gcc')

clw.close()

