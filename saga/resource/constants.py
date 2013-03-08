
# resource type enum
COMPUTE      =  1,            # accepting jobs
NETWORK      =  2,            # connectes compute(s) and storage(s)
STORAGE      =  3,            # mounted on / accessible by compute
POOL         =  4,            # collection of resources

# resource state enum
UNKNOWN      =  None          # wut?
PENDING      =  1,            # will become active eventually
ACTIVE       =  2,            # accepting jobs, jobs can run
DRAINING     =  4,            # jobs still run, not accepting new jobs
RUNNING      = PENDING | ACTIVE | DRAINING
CLOSED       =  8,            # closed by user
EXPIRED      = 16,            # closed by system
FAILED       = 32,            # closed unexpectedly by system or internally
FINAL        = CLOSED | EXPIRED | FAILED

# resource attributes
ID           = 'ID'           # url
TYPE         = 'TYPE'         # type enum
STATE        = 'STATE'        # state enum
STATE_DETAIL = 'STATE_DETAIL' # string
MANAGER      = 'MANAGER'      # url
DESCRIPTION  = 'DESCRIPTION ' # dict

# generic resource description attributes
TYPE         = 'Type'         # enum           - reuired
TEMPLATE     = 'Template'     # list<string>

# resource lifetime attributes
DYNAMIC      = 'Dynamic'      # bool
START        = 'Start'        # time
END          = 'End'          # time
DURATION     = 'Duration'     # time

# resource type specific (non-generic) attributes
MACHINE_OS   = 'MachineOS'    # enum
MACHINE_ARCH = 'MachineArch'  # enum
NAMES        = 'Names'        # list<string>
SIZE         = 'Size'         # int            - #slots, size in MB, #IPs
MEMORY       = 'Memory'       # int
ACCESS       = 'Access'       # string         - fqhn/ip, network device, 
                              #                - mnt point or provisioning url


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

