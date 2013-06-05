
""" libcloud based EC2 resource adaptor """

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
_ADAPTOR_NAME          = "saga.adaptor.ec2_resource"
_ADAPTOR_SCHEMAS       = ["ec2", "ec2_keypair"]
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
    "contexts"         : {"ec2"         : "EC2 ID and Secret",
                          "ec2_keypair" : "ec2 keypair for node access"}
}

# --------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC           = {
    "name"             : _ADAPTOR_NAME,
    "cfg_options"      : _ADAPTOR_OPTIONS, 
    "capabilities"     : _ADAPTOR_CAPABILITIES,
    "description"      : """ 
        The EC2 resource adaptor. This adaptor interacts with a variety of
        IaaS backends via the Apache LibCloud.  It also provides EC2 related
        context types.
        """,
    "example": "examples/jobs/resource_ec2.py",
    "schemas"          : {"ec2"         : "Amacon EC2 key/secret",
                          "ec2_keypair" : "Amacon EC2 keypair name"}
}

# --------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA

_ADAPTOR_INFO          = {
    "name"             : _ADAPTOR_NAME,
    "version"          : "v0.1",
    "schemas"          : _ADAPTOR_SCHEMAS,
    "cpis"             : [
        { 
        "type"         : "saga.Context",
        "class"        : "EC2Context"
        }, 
        { 
        "type"         : "saga.Context",
        "class"        : "EC2Keypair"
        }, 
        { 
        "type"         : "saga.resource.Manager",
        "class"        : "EC2ResourceManager"
        }, 
        { 
        "type"         : "saga.resource.Compute",
        "class"        : "EC2ResourceCompute"
        },
    ]
}

###############################################################################
# The adaptor class

class Adaptor (saga.adaptors.base.Base):
    """
    use 'export LIBCLOUD_DEBUG=/dev/stderr' for debugging
    """

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
            import saga.utils.misc as sutm

            # get the libclound modules.  Note that the non-empty fromlist
            # forces Python to include the actually specified module, not only
            # the top level libcloud.  Oh Python...
            self.lc   = __import__ ('libcloud',                   fromlist=[''])
            self.lcct = __import__ ('libcloud.compute.types',     fromlist=[''])
            self.lccp = __import__ ('libcloud.compute.providers', fromlist=[''])

            # we only tested against libcloud 0.12.4, so we require that version
            # for now, at least
            if  sutm.normalize_version (self.lc.__version__) < \
                sutm.normalize_version ('0.12.4')       :
                raise saga.NoSuccess ("Libcloud version newer than 0.12.4 required")


            # 0.12.4 does not support keypair footprint inspection, so we cannot
            # create ssh keys from keypair names.  We check for that feature
            # here, and issue a warning otherwise
            # for now, at least
            if  sutm.normalize_version (self.lc.__version__) < \
                sutm.normalize_version ('0.12.5')       :
                self._logger.warning ("Libcloud version does not allow keypair "
                                      "inspection -- cannot associate ssh keys "
                                      "with ec2 keypairs")
                self.lc_has_footprint = False
            else :
                self.lc_has_footprint = True


        except Exception as e :
            self._logger.warning ("Could not load libcloud module, "
                                  "disable EC2 resource adaptor")
            self._logger.warning (str(e))
            raise saga.NoSuccess ("Cannot load libcloud")


    # ----------------------------------------------------------------
    #
    def _get_default_contexts (self) :

        # no default keypair in ec2
        # FIXME: pick up a default context from the EC2 default env vars
        return []


    # ----------------------------------------------------------------
    #
    def parse_id (self, id) :
        # split the id '[manager-url]-[resource-url]' in its parts, and return them.

        match = self.id_re.match (id)

        if  not match or len (match.groups()) != 2 :
            raise saga.BadParameter ("Cannot parse resource id '%s'" % id)

        return (saga.Url (match.group(1)), str (match.group (2)))



###############################################################################
#
class EC2Keypair (saga.adaptors.cpi.context.Context) :

    """ 
    This context points to an EC2 keypair which is used to contextualize VM
    instances.

    The context can be used in two ways, depending on the specified keys:

    *Version 1:* reference an existing (uploaded) keypair:
      - `Token`  : name of keypair to be used  (required)
      - `UserID` : username on VM instance     (optional, default 'root')
      - `Server` : authentication server host  (optional, default for Amazon)

    A 'DoesNotExist' exception will be raised if the referenced keypair does not
    exist, and the context will not be added to the session.  An attempt to
    connect to a VM with an invalid keypair (i.e.  a keypair not registered upon
    VM creation), or with an invalid user id, will result in an
    'AuthorizationDenied' exception.


    *Version 2:* create (upload) a new keypair, and the use it
      - `Token`  : name of keypair to create   (required)
      - `UserKey`: public  ssh key             (required)
      - `UserID` : username on VM instance     (optional, default 'root') 
      - `Server` : authentication server host  (optional, default for Amazon)

    When used in this version, the adaptor will attempt to create an EC2 keypair
    with the given name (`Token`), by uploading the public ssh key.  On success,
    the `UserKey` attribute will then be removed from the context, to avoid
    repeated uploads on re-use, and the context will behave as in Version 1.  If
    a keypair with the given name already exists, an 'AlreadyExists' exception
    is raised.  All other errors with result in a `NoSuccess` exception.  If any
    error occurs, the context will not be added to the session.


    Known Limitations
    =================

    1) For a given EC2 keypair name, we should fetch the respective key footprint
    with `conn.ex_describe_keypairs('self.api.target')`, then sift through all
    public ssh keys we can find, and see if one matches that footprint.  If one
    does, we should add that respective ssh key to the session, so that it can
    be used for host access authentication.

    Alas, the `ex_describe_keypairs()` call is faulty and does not return
    footprints, and so we have no chance really to find the respective ssh key.
    We thus need to rely on the user to add the respective ssh key to the
    session on her own.  

    I have filed a bug report with libcloud[1], lets see what happens.  At this
    point, this context adaptor does basically nothing than hosting the EC2
    keypair for session authentication.  

    Update: It turns out that EC2 uses a non-standard and (worse) non-specified
    way to compute the fingerprints, and we can't use those to select local
    keys.  There apparently exists a feature-request to AWS to switch to normal
    ssl MD5 digests [2], but it is unclear if and when that happens.  That
    severely impacts our ability to choose a suitable ssh key for VM access, and
    we must rely on the user to provide a suitable one, either in the
    ec2_keypair context, or in a separate ssh context.


    [1] `https://issues.apache.org/jira/browse/LIBCLOUD-326`
    [2] `https://forums.aws.amazon.com/message.jspa?messageID=386571`


    2) The `Server` attribute is ignored at this point, only the default AWS
    service endpoint is supported (the default defined by libcloud).

    3) EC2 reports the VM instance to be 'Running' when it starts booting -- at
    that point the ssh login is not yet functional, and job service instance
    creation will thus fail.  I don't see a simple way to inspect the internal
    state (apart from trial/error which is not implemented), so the application
    needs to cater for that, for example by re-trying to connect.

    """


    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        _cpi_base = super  (EC2Keypair, self)
        _cpi_base.__init__ (api, adaptor)

        self.lcct = self._adaptor.lcct
        self.lccp = self._adaptor.lccp


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, type) :
        
        if  not type.lower () == 'ec2'         and \
            not type.lower () == 'ec2_keypair' :
            raise saga.exceptions.BadParameter \
                  ("ec2 adaptor only handles 'ec2' and 'ec2_keypair' contexts")

        self._type = type

        return self


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def _initialize (self, session) :

        if  self._type == 'ec2' :
            # nothing to do for simple ec2 id/secret containers
            # FIXME: we could in principle validate validity...
            return 

        # we have an ec2_keypair context.  We need to find an ec2  context in
        # the session though which we can use to talk to the backend
        self.ec2_id  = None
        self.ec2_key = None

        for ctx in session.contexts :
            if  ctx.type == 'ec2' :
                self.ec2_id  = ctx.user_id
                self.ec2_key = ctx.user_key

        if  not self.ec2_id or not self.ec2_key :
            raise saga.exceptions.AuthenticationFailed \
                  ("cannot initialize 'ec2_keypair' withough ec2 context")

        if  not self._api ().token :
            raise saga.exceptions.BadParameter \
                  ("'ec2_keypair' context must speciy keypair name as 'Token'")

        # valid context, connect to backend  
        # FIXME: use 'Server' if defined
        driver = self.lccp.get_driver (self.lcct.Provider.EC2)
        conn   = driver (self.ec2_id, self.ec2_key)

        # check if given keypair exists.  We only can do that by inspecting it,
        # and capturing an eventual exception.
        keypair = None
        token   = self._api ().token
        key     = self._api ().user_key
        create  = False

        try:
            keypair = conn.ex_describe_keypairs (token)

        except Exception as e :

            e_str = str(e)

            if e_str.startswith ("InvalidKeyPair.NotFound") :

                if  not key :
                    raise saga.exceptions.BadParameter \
                          ("'ec2_keypair' not found: %s" % e)

                # keypair not found, but we have a key, so we can register it!
                create = True

            else :
                raise saga.exceptions.BadParameter \
                      ("'ec2_keypair' not found: %s" % e)


        if  create and key :

            self._logger.info ("import new keypair %s : %s" % (token, key))

            try :
                keypair = conn.ex_import_keypair (token, key)

            except Exception as e :
                raise saga.exceptions.BadParameter \
                      ("'ec2_keypair' not found: %s" % e)

            # import worked -- we don't need to import again, so unset the
            # user_key attribute
            self._api ().user_key = None


        elif create and not key : 
            self._logger.warning ("ignore 'UserKey' attribute for existing 'ec2_keypair'")


        # *Version 1:* reference an existing (uploaded) keypair:
        #   - `Token`  : name of keypair to be used  (required)
        #   - `UserID` : username on VM instance     (optional, default 'root')
        #   - `Server` : authentication server host  (optional, default for Amazon)

        # *Version 2:* create (upload) a new keypair, and the use it
        #   - `Token`  : name of keypair to create   (required)
        #   - `UserKey`: public  ssh key             (required)
        #   - `UserID` : username on VM instance     (optional, default 'root') 
        #   - `Server` : authentication server host  (optional, default for Amazon)
        #
        





###############################################################################
#
class EC2ResourceManager (saga.adaptors.cpi.resource.Manager) :

    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (EC2ResourceManager, self)
        self._cpi_base.__init__ (api, adaptor)

        self.lcct = self._adaptor.lcct
        self.lccp = self._adaptor.lccp



    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, url, session) :

        self.url     = saga.Url (url)  # deep copy
        self.session = session

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
                self.url.host != 'aws.amazon.com' :
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


        # check if a any 'ec2_keypair' context is known.  If so, use its
        # 'keypair'
        # attribute as keypair name for node creation
        token = ''
        for context in self.session.contexts  :
            if  context.type == 'ec2_keypair' :
                token = context.token
                self._logger.info ("using '%s' as ec2 keypair" % token)
       
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
                                              image = self.images_dict[rd.image], 
                                              ex_keyname = token)

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
    #
    @SYNC_CALL
    def acquire_by_id (self, rid) :

        if  not self.conn :
            raise saga.IncorrectState ("not connected to backend")

        resource_info = None

        if  self.backend == 'amazon.ec2' :

            if True :
          # try :
                
                manager_url, rid_s = self._adaptor.parse_id (str(rid))

                # FIXME: interpret / verify size
                nodes  = self.conn.list_nodes (ex_node_ids=[rid_s])

                if  len (nodes) < 1 :
                    raise saga.BadParameter ("Cannot find resource '%s'" % rid_s)
                if  len (nodes) > 1 :
                    raise saga.BadParameter ("Cannot identify resource '%s'" % rid_s)

                node = nodes[0]

                resource_info = { 'backend'                 : self.backend   ,
                                  'resource'                : node           ,
                                  'resource_type'           : COMPUTE        ,
                                  'resource_description'    : None           ,
                                  'resource_manager'        : self.get_api (), 
                                  'resource_manager_url'    : self.url       , 
                                  'resource_schema'         : self.url.schema, 
                                  'connection'              : self.conn      }

          # except Exception as e :
          #     # FIXME: translate errors more sensibly
          #     raise saga.NoSuccess ("Failed with %s" % e)

        if  resource_info :
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
    def destroy (self, id):

        node = self.aquire (id)
        node.destroy ()

   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def list_templates (self, rtype) :

        # we support only compute templates right now
        if  rtype and not (rtype & COMPUTE) :
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
        if  rtype and not (rtype & COMPUTE) :
            return []

        if not len (self.images) :
            self._refresh_images ()

        return self.images

   

###############################################################################
#
class EC2ResourceCompute (saga.adaptors.cpi.resource.Compute) :

    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (EC2ResourceCompute, self)
        self._cpi_base.__init__ (api, adaptor)

        self.lcct = self._adaptor.lcct
        self.lccp = self._adaptor.lccp

        self.state       = NEW
        self.detail      = None
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

            self.rid    = self.resource.id
            self.id     = "[%s]-[%s]" % (self.manager_url, self.rid)
            self.access = None

            if  self.backend != 'amazon.ec2' :
                raise saga.BadParameter ("not support for %s" % self.backend)


            # FIXME: we don't actually need new state, it should be fresh at
            # this point, right?!
            self._refresh_state ()



        else :
            raise saga.BadParameter ("Cannot acquire resource, no contact information")


        return self.get_api ()


    # --------------------------------------------------------------------------
    #
    def _refresh_state (self) :

        if  self.state == EXPIRED :
            # no need to update, state is final
            return

        try :
            # NOTE: ex_node_ids is only supported by ec2
            nodes = self.conn.list_nodes (ex_node_ids=[self.rid])

            if  not len (nodes) :
                raise saga.IncorrectState ("resource '%s' disappeared")

            if  len (nodes) != 1 :
                self._log.warning ("Could not uniquely identify instance for '%s'" % self.rid)

            self.resource = nodes[0]

            if  'status' in self.resource.extra :
                self.detail = self.resource.extra['status']

            # FIXME: move state translation to adaptor
            if   self.resource.state == self.lcct.NodeState.RUNNING    : self.state = ACTIVE
            elif self.resource.state == self.lcct.NodeState.REBOOTING  : self.state = PENDING
            elif self.resource.state == self.lcct.NodeState.TERMINATED : self.state = EXPIRED
            elif self.resource.state == self.lcct.NodeState.PENDING    : self.state = PENDING
            elif self.resource.state == self.lcct.NodeState.UNKNOWN    : self.state = UNKNOWN
            else                                                       : self.state = UNKNOWN

            if  self.state == UNKNOWN and self.detail == 'shutting-down' :
                self.state =  EXPIRED

        except Exception as e :
            self._logger.error ("Could not obtain resource state (%s): %s" \
                             % (self.id, e))
            self.state  =  UNKNOWN

        if  self.state  == EXPIRED :
            self.access == None
        else :
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
    def destroy (self):

        self.conn.destroy_node (self.resource)
        self.state  = EXPIRED
        self.detail = 'destroyed by user'


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def wait (self, state, timeout) : 

        import time
        start = time.time ()

        while not self.state :
            self._refresh_state ()

        if  self.state == EXPIRED and \
            not  state  & EXPIRED :      
                raise saga.IncorrectState ("resource is in final state (%s): %s" \
                                        % (self.detail, self.id))

        while not ( self.state & state ) :

            self._logger.info ("wait   for resource state %s: %s" % (state, self.state))

            if timeout > 0 :
                now = time.time ()

                if  (now - start > timeout) :
                    break

            elif timeout == 0 :
                break

            self._refresh_state ()

        self._logger.info ("waited for resource state %s: %s" % (state, self.state))

        return
    


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

