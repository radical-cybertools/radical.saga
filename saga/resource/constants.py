
# FIXME: OS enums, ARCH enums

# resource type enum
COMPUTE      =  1,            """ resource accepting jobs """
STORAGE      =  2,            """ storage resource (duh) """
NETWORK      =  3,            """ connects compute and storage resources """

# resource state enum """
UNKNOWN      =  None          """ wut? """
NEW          =  1,            """ requsted, not accepting jobs, yet;
                                  initial state """
PENDING      =  2,            """ accepting jobs, will become active eventually """
ACTIVE       =  4,            """ accepting jobs, jobs can run """
DRAINING     =  8,            """ jobs still run, not accepting new jobs """
RUNNING      = PENDING  | ACTIVE | DRAINING
CANCELED     = 16,            """ released by user; final state """
EXPIRED      = 32,            """ released by system; final state """
DONE         = EXPIRED,       """ alias """
FAILED       = 64,            """ released unexpectedly by system or internally;
                                  final state """
FINAL        = CANCELED | DONE   | FAILED

# resource attributes """
ID           = 'ID'           """ url identifying a resource instance """
TYPE         = 'TYPE'         """ type enum, identifying the resource type """
STATE        = 'STATE'        """ state enum, identifying the rsource state """
STATE_DETAIL = 'STATE_DETAIL' """ string, representing the native backend state """
MANAGER      = 'MANAGER'      """ url, pointing to the resource's manager """
DESCRIPTION  = 'DESCRIPTION ' """ dict, containing resource attributes  """

# generic resource description attributes """
TEMPLATE     = 'Template'     """ string, template to which the resource 
                                  was created"""

# resource lifetime attributes """
DYNAMIC      = 'Dynamic'      """ bool, enables/disables on-demand 
                                  resource 
                                  resizing """
START        = 'Start'        """ time, expected time at which resource 
                                  becomes ACTIVE """
END          = 'End'          """ time, expected time at which resource 
                                  will EXPIRE """
DURATION     = 'Duration'     """ time, expected time span between ACTIVE 
                                  and EXPIRED """

# resource type specific (non-generic) attributes """
MACHINE_OS   = 'MachineOS'    """ enum, identifying the resource's operating 
                                  system """
MACHINE_ARCH = 'MachineArch'  """ enum, identifying the machine architecture """
SIZE         = 'Size'         """ int, identifying No. of slots / size in 
                                  MB / No. of IPs """
MEMORY       = 'Memory'       """ int, identifying memory size in MegaByte """
ACCESS       = 'Access'       """ string, identifying the hostname/ip, mount
                                  point or provisioning URL to access the
                                  resource """


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

