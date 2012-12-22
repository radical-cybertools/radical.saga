
# replica flags enum:
OVERWRITE      =    1
RECURSIVE      =    2
DEREFERENCE    =    4
CREATE         =    8
EXCLUSIVE      =   16
LOCK           =   32
CREATE_PARENTS =   64
READ           =  512
WRITE          = 1024
READ_WRITE     = 1536


from saga.replica.file       import LogicalFile
from saga.replica.directory  import LogicalDirectory

