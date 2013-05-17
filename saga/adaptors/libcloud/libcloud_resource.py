
""" libcloud based resource adaptor implementation """

import saga.adaptors.cpi.base
import saga.adaptors.cpi.resource

from   saga.resource.constants import *
ANY = COMPUTE | STORAGE

import re
import os
import time
import threading

SYNC_CALL  = saga.adaptors.cpi.decorators.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.decorators.ASYNC_CALL


# --------------------------------------------------------------------
# the adaptor info
#
_ADAPTOR_NAME          = "saga.adaptor.libcloud_resource"
_ADAPTOR_SCHEMAS       = ["ec2"]
_ADAPTOR_OPTIONS       = []

# --------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES  = {
    "rdes_attributes"  : [saga.resource.RTYPE         ,
                          saga.resource.TEMPLATE      ,
                          saga.resource.IMAGE         ,
                          saga.resource.MACHINE_OS    ,
                          saga.resource.MACHINE_ARCH  ,
                          saga.resource.SIZE          ,
                          saga.resource.MEMORY        ,
                          saga.resource.ACCESS       ],
    "res_attributes"   : [saga.resource.RTYPE         ,
                          saga.resource.TEMPLATE      ,
                          saga.resource.IMAGE         ,
                          saga.resource.MACHINE_OS    ,
                          saga.resource.MACHINE_ARCH  ,
                          saga.resource.SIZE          ,
                          saga.resource.MEMORY        ,
                          saga.resource.ACCESS       ],    
    "metrics"          : [saga.resource.STATE, 
                          saga.resource.STATE_DETAIL],
    "contexts"         : {"ssh"      : "public/private keypair",
                          "x509"     : "X509 proxy",
                          "userpass" : "username/password pair"}
}

# --------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC           = {
    "name"             : _ADAPTOR_NAME,
    "cfg_options"      : _ADAPTOR_OPTIONS, 
    "capabilities"     : _ADAPTOR_CAPABILITIES,
    "description"      : """ 
        The LibCloud resource adaptor. This adaptor interacts with a variety of
        IaaS backends via the Apache LibCloud.
        """,
    "example": "examples/jobs/localresource.py",
    "schemas"          : {"ec2"  : "Amacon EC2"}
}

# --------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA

_ADAPTOR_INFO          = {
    "name"             : _ADAPTOR_NAME,
    "version"          : "v0.1",
    "schemas"          : _ADAPTOR_SCHEMAS,
    "cpis"             : [
        { 
        "type"         : "saga.resource.Manager",
        "class"        : "LibcloudResourceManager"
        }, 
        { 
        "type"         : "saga.resource.Compute",
        "class"        : "LibcloudResourceCompute"
        },
    ]
}

###############################################################################
# The adaptor class

class Adaptor (saga.adaptors.base.Base):

    # ----------------------------------------------------------------
    #
    def __init__ (self) :

        self.base = super  (Adaptor, self)
        self.base.__init__ (_ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        # for id parsing
        self.id_re = re.compile ('^\[(.*)\]-\[(.*?)\]$')


    # ----------------------------------------------------------------
    #
    def sanity_check (self) :

        try :
            # get the libclound modules.  Note that the non-empty fromlist
            # forces Python to include the actually specified module, not only
            # the top level libcloud.  Oh Python...
            self.lcct = __import__ ('libcloud.compute.types',     fromlist=[''])
            self.lccp = __import__ ('libcloud.compute.providers', fromlist=[''])


        except Exception as e :
            self._logger.warning ("Could not load libcloud module, "
                                  "disable libcloud resource adaptor")
            self._logger.warning (str(e))
            raise saga.NoSuccess ("Cannot load libcloud")


    # ----------------------------------------------------------------
    #
    def parse_id (self, id) :
        # split the id '[manager-url]-[resource-url]' in its parts, and return them.

        print id
        match = self.id_re.match (id)

        if  not match or len (match.groups()) != 2 :
            raise saga.BadParameter ("Cannot parse resource id '%s'" % id)

        return (saga.Url (match.group(1)), saga.Url (match.group (2)))




###############################################################################
#
class LibcloudResourceManager (saga.adaptors.cpi.resource.Manager) :

    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (LibcloudResourceManager, self)
        self._cpi_base.__init__ (api, adaptor)

        self.lcct = self._adaptor.lcct
        self.lccp = self._adaptor.lccp



    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, url, session) :

        self.url     = saga.Url (url)  # deep copy
        self.session = session

        print self.lccp

        # internale (cached) registry of available resources
        self.templates       = []
        self.templates_dict  = {}
        self.images          = []
        self.images_dict     = {}
        self.access          = {}
        self.access[COMPUTE] = []
        self.access[STORAGE] = []
        self.access[ANY]     = []

        self.backend = None
        self.driver  = None
        self.conn    = None

        if  self.url.schema == 'ec2' :
            if  self.url.host and \
                self.url.host != 'aws.amaon.com' :
                raise saga.BadParameter ("only amazon/EC2 supported (not %s)" \
                                      % self.url)

            self.backend = 'amazon.ec2'

            # FIXME: support proper contexts, and also default EC2 env vars
            self.ec2_id  = os.environ['EC2_ID']
            self.ec2_key = os.environ['EC2_KEY']
            
            # FIXME: translate exceptions, in particular connectivity and auth
            # exceptions.
            self.driver = self.lccp.get_driver (self.lcct.Provider.EC2)
            self.conn   = self.driver (self.ec2_id, self.ec2_key)
            print (self.conn)

            self.templates = []
            self.images    = []

            # FIXME: we could pre-fetch existing resources right now...
            
        else :
            raise saga.BadParameter ( "only EC2 is supported (not %s)" \
                                  % self.url)


    # ----------------------------------------------------------------
    #
    def _refresh_templates (self, pattern=None) :

        self.templates      = []
        self.templates_dict = {}

        for template in self.conn.list_sizes (pattern) :

            self.templates_dict   [template.name] = template
            self.templates.append (template.name)


    # ----------------------------------------------------------------
    #
    def _refresh_images (self, pattern=None) :

        self.images      = []
        self.images_dict = {}

        for image in self.conn.list_images (pattern) :

            if  image.id.startswith ('ami-') :

                self.images_dict   [image.id] = image
                self.images.append (image.id)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def acquire (self, rd) :

        if  not self.conn :
            raise saga.IncorrectState ("not connected to backend")

        if  rd.rtype != COMPUTE :
            raise saga.BadParameter ("can only acquire compute resources.")

       
        resource_info = None

        # check that only supported attributes are provided
        for attribute in rd.list_attributes():
            if attribute not in _ADAPTOR_CAPABILITIES["rdes_attributes"]:
                msg = "'resource.Description.%s' is not supported by this adaptor" % attribute
                raise saga.BadParameter._log (self._logger, msg)


        if  self.backend == 'amazon.ec2' :
            # for amazon EC2, we only support template defined instances
            if  not rd.template :
                raise saga.BadParameter ("no 'template' attribute in resource description")
            
            # we also need an OS image
            if  not rd.image :
                raise saga.BadParameter ("no 'image' attribute in resource description")

            # and we don't support any other attribute right now
            if  rd.dynamic      or rd.start        or \
                rd.end          or rd.duration     or \
                rd.machine_os   or rd.machine_arch or \
                rd.access       or rd.memory       :
                raise saga.BadParameter ("amazon.ec2 resource descriptions only "
                                         "supports 'template' and 'image' attributes")

            if True :
          # try :
                

                # make sure template and image are valid, and get handles
                if  not rd.template in self.templates_dict : 
                    self._refresh_templates (rd.template)

                if  not rd.image in self.images_dict : 
                    self._refresh_images (rd.image)


                # FIXME: interpret / verify size

                # it should be safe to create the VM instance now
                node = self.conn.create_node (name  = 'saga.resource.Compute',
                                              size  = self.templates_dict[rd.template], 
                                              image = self.images_dict[rd.image])

                resource_info = { 'backend'                 : self.backend   ,
                                  'resource'                : node           ,
                                  'resource_type'           : rd.rtype       ,
                                  'resource_description'    : rd             ,
                                  'resource_manager'        : self.get_api (), 
                                  'resource_manager_url'    : self.url       , 
                                  'resource_schema'         : self.url.schema, 
                                  'connection'              : self.conn      }

          # except Exception as e :
          #     # FIXME: translate errors more sensibly
          #     raise saga.NoSuccess ("Failed with %s" % e)

        if  resource_info :
            if  rd.rtype == COMPUTE :
                return saga.resource.Compute (_adaptor       = self._adaptor, 
                                              _adaptor_state = resource_info)

        raise saga.NoSuccess ("Could not acquire requested resource")


    # ----------------------------------------------------------------
    @SYNC_CALL
    def get_url (self) :

        return self.url


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def list (self, rtype):

        # FIXME
        return self.access[rtype]
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def release (self, id):

        # FIXME
        return # hahaha

   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def list_templates (self, rtype) :

        # we support only compute templates right now
        if  rtype and not rtype | COMPUTE :
            return []

        if not len (self.templates) :
            self._refresh_templates ()
    
        return self.templates

   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_template (self, name) :

        # FIXME
        raise saga.BadParameter ("unknown template %s" % name)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def list_images (self, rtype) :

        # we support only compute images right now
        if  rtype and not rtype | COMPUTE :
            return []

        if not len (self.images) :
            self._refresh_images ()

        return self.images

   

###############################################################################
#
class LibcloudResourceCompute (saga.adaptors.cpi.resource.Compute) :

    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (LibcloudResourceCompute, self)
        self._cpi_base.__init__ (api, adaptor)

        self.lcct = self._adaptor.lcct
        self.lccp = self._adaptor.lccp

        self.state       = NEW
        self.rid         = None
        self.rtype       = None
        self.manager     = None
        self.manager_url = None


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_info, id, session):

        # eval id if given
        if  id :
            # FIXME
            self.manager_url, self.rid = self._adaptor.parse_id (id)
            self.manager = saga.resource.Manager (self.manager_url)

            if  self.rid in self.manager.list (COMPUTE) :
                self.rtype = COMPUTE

            else :
                raise saga.BadParameter ("Cannot handle resource type for %s", id)

        # no id -- grab info from adaptor_info
        elif adaptor_info :

            if  not 'backend'              in adaptor_info or \
                not 'resource'             in adaptor_info or \
                not 'resource_type'        in adaptor_info or \
                not 'resource_description' in adaptor_info or \
                not 'resource_manager'     in adaptor_info or \
                not 'resource_manager_url' in adaptor_info or \
                not 'connection'           in adaptor_info    :
                raise saga.BadParameter ("Cannot acquire resource, insufficient information")

            self.backend     = adaptor_info['backend']
            self.resource    = adaptor_info['resource']
            self.rtype       = adaptor_info['resource_type']
            self.descr       = adaptor_info['resource_description']
            self.manager     = adaptor_info['resource_manager']
            self.manager_url = adaptor_info['resource_manager_url']
            self.conn        = adaptor_info['connection']

            print " --------------------- "
            print type (self.resource)
            print self.resource

            import pprint
            pprint.pprint (self.resource.__dict__)
        
            self.rid    = self.resource.id
            self.id     = "[%s]-[%s]" % (self.manager_url, self.rid)
            self.access = None

            if  self.backend != 'amazon.ec2' :
                raise saga.BadParameter ("not support for %s" % self.backend)


            # FIXME: we don't actually need new state, it should be fresh at
            # this point...
            self._refresh_state ()



        else :
            raise saga.BadParameter ("Cannot acquire resource, no contact information")


        return self.get_api ()


    # --------------------------------------------------------------------------
    #
    def _refresh_state (self) :

        # NOTE: ex_node_ids is only supported by ec2
        nodes = self.conn.list_nodes (ex_node_ids=[self.rid])

        if  not len (nodes) :
            raise saga.IncorrectState ("resource '%s' disappeared")

        if  len (nodes) != 1 :
            self._log.warning ("Could not uniquely identify instance for '%s'" % self.rid)

        self.resource = nodes[0]

        # FIXME: move state translation to adaptor
        if   self.resource.state == self.lcct.NodeState.RUNNING    : self.state = ACTIVE
        elif self.resource.state == self.lcct.NodeState.REBOOTING  : self.state = PENDING
        elif self.resource.state == self.lcct.NodeState.TERMINATED : self.state = EXPIRED
        elif self.resource.state == self.lcct.NodeState.PENDING    : self.state = PENDING
        elif self.resource.state == self.lcct.NodeState.UNKNOWN    : self.state = UNKNOWN
        else                                                       : self.state = UNKNOWN

        if  'status' in self.resource.extra :
            self.detail = self.resource.extra['status']

        if  len (self.resource.public_ips) :
            self.access = "ssh://%s/" % self.resource.public_ips[0]


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_id (self) : 
        
        return self.id


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_rtype (self) : 
        
        return self.rtype


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state (self) : 
        
        return self.state


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state_detail (self) : 
        
        return self.detail


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_access (self) : 

        if  not self.access :
            self._refresh_state ()

        return self.access


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_manager      (self) : 
        
        return self.manager


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_description  (self) : 
        
        return self.descr


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def reconfig (self):
        raise saga.NotImplemented ("This backend cannot reconfigre resources")


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def release (self):

        return self.manager.release (self.id)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def wait (self, state, timeout) : 
        # trick is, we *never* change state...

        import time
        start = time.time ()

        while not ( self.state | state ) :

            if timeout > 0 :
                now = time.time ()

                if  (now - start > timeout) :
                    break

            elif timeout == 0 :
                break

            self._refresh_state ()

        return
    


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

