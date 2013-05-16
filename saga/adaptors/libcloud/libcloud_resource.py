
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
                          saga.resource.MACHINE_OS    ,
                          saga.resource.MACHINE_ARCH  ,
                          saga.resource.SIZE          ,
                          saga.resource.MEMORY        ,
                          saga.resource.ACCESS       ],
    "res_attributes"   : [saga.resource.RTYPE         ,
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
            import libcloud.compute.types      as lcct
            import libcloud.compute.providers  as lccp

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



    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, url, session) :

        self.url     = saga.Url (url)  # deep copy
        self.session = session

        # get the libclound modules.  Note that the non-empty fromlist forces
        # Python to include the actually specified module, not only the top
        # level libcloud.  Oh Python...
        #
        # FIXME: can be cached in the adaptor, not sure if Python cares...
        self.lcct = __import__ ('libcloud.compute.types',     fromlist=[''])
        self.lccp = __import__ ('libcloud.compute.providers', fromlist=[''])

        print self.lccp

        # internale (cached) registry of available resources
        self.access  = {}
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

            # FIXME: we could pre-fetch existing resources right now...
            
            # FIXME: we definitely should pre-fetch templates, and actually
            # cache those in the adaptor (we assume that list to be stable over
            # application lifetime).


        else :
            raise saga.BadParameter ( "only EC2 is supported (not %s)" \
                                  % self.url)


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
                rd.size         or rd.memory       or \
                rd.access       :
                raise saga.BadParameter ("amazon.ec2 resource descriptions only"
                                         "support 'template' and 'image' attributes")

            resource      = self.conn.create_node (image=rd.image, size=rd.template)
            resource_info = { "resource_manager"        : self.get_api (), 
                              "resource_manager_url"    : self.url       , 
                              "resource_type"           : rd.rtype       ,
                              "resource"                : resource       ,
                              "connection"              : self.conn      ,
                              "backend"                 : self.backend   }

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

        # FIXME
        return [] # no templates

   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_template (self, name) :

        # FIXME
        raise saga.BadParameter ("unknown template %s" % name)


###############################################################################
#
class LibcloudResourceCompute (saga.adaptors.cpi.resource.Compute) :

    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (LibcloudResourceCompute, self)
        self._cpi_base.__init__ (api, adaptor)

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
                not 'connection'           in adaptor_info or \
                not 'resource_manager'     in adaptor_info or \
                not 'resource_manager_url' in adaptor_info    :
                raise saga.BadParameter ("Cannot acquire resource, insufficient information")

            self.backend     = adaptor_info['backend']
            self.conn        = adaptor_info['connection']
            self.rtype       = adaptor_info['resource_type']
            self.resource    = adaptor_info['resource']
            self.manager     = adaptor_info['resource_manager']
            self.manager_url = adaptor_info['resource_manager_url']
        
            self.rid    = self.resource['instanceId']
            self.access = "ssh://%s/" % self.resource ['dns_name']
            self.id     = "[%s]-[%s]" % (self.manager_url, self.rid)

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

        node = self.conn.list_nodes (ids=[self.rid])

        self.detail = self.resource ['status']

        # FIXME: move state translation to adaptor
        # pending | running | shutting-down | terminated | stopping | stopped
        if   self.detail == 'pending'       : self.state = PENDING
        elif self.detail == 'running'       : self.state = ACTIVE
        elif self.detail == 'shutting-down' : self.state = EXPIRED
        elif self.detail == 'stopping'      : self.state = CANCELED
        elif self.detail == 'stopped'       : self.state = DONE
        elif self.detail == 'terminated'    : self.state = DONE 
        else                                : self.state = UNKNOWN


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_id           (self) : return self.id

    @SYNC_CALL
    def get_rtype        (self) : return self.rtype

    @SYNC_CALL
    def get_state        (self) : return self.state

    @SYNC_CALL
    def get_state_detail (self) : return self.detail

    @SYNC_CALL
    def get_access       (self) : return self.access

    @SYNC_CALL
    def get_manager      (self) : return self.manager

    @SYNC_CALL
    def get_description  (self) : return { ACCESS : self.access_url }

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

