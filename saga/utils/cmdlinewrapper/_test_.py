from cmdlinewrapper import CommandLineWrapper

clw = CommandLineWrapper.init_as_subprocess_wrapper(None)
clw.open()
print clw.run_sync('/bin/sleep', ['2'])
clw.close()