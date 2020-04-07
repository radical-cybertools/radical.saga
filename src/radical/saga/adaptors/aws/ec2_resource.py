
__author__    = "Andre Merzky, Ashley Z, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" libcloud based EC2 resource adaptor """

import re
import os

from ...                   import exceptions as rse
from ...                   import url        as rs_url
from ...                   import session    as rs_session
from ...                   import context    as api_context
from ...                   import resource   as api_resource
from ..                    import base       as a_base
from ..cpi                 import context    as cpi_context
from ..cpi                 import resource   as cpi_resource
from ..cpi                 import decorators as cpi_decs
from ...resource           import constants  as c

c.ANY = c.COMPUTE | c.STORAGE

SYNC_CALL  = cpi_decs.SYNC_CALL
ASYNC_CALL = cpi_decs.ASYNC_CALL


# ------------------------------------------------------------------------------
# the adaptor info
#
_ADAPTOR_NAME          = "radical.saga.adaptors.ec2_resource"
_ADAPTOR_SCHEMAS       = ["ec2", "ec2_keypair", "openstack", "eucalyptus",
                          "euca", "aws", "amazon", "http", "https"]
_ADAPTOR_OPTIONS       = []

# ------------------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES  = {
    "rdes_attributes"  : [c.RTYPE         ,
                          c.TEMPLATE      ,
                          c.IMAGE         ,
                          c.DYNAMIC       ,
                          c.MACHINE_OS    ,
                          c.MACHINE_ARCH  ,
                          c.SIZE          ,
                          c.MEMORY        ,
                          c.ACCESS       ],
    "res_attributes"   : [c.RTYPE         ,
                          c.TEMPLATE      ,
                          c.IMAGE         ,
                          c.MACHINE_OS    ,
                          c.MACHINE_ARCH  ,
                          c.SIZE          ,
                          c.MEMORY        ,
                          c.ACCESS       ],
    "metrics"          : [c.STATE,
                          c.STATE_DETAIL],
    "contexts"         : {"ec2"         : "EC2 ID and Secret",
                          "ec2_keypair" : "ec2 keypair for node access"}
}

# ------------------------------------------------------------------------------
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
    "example": "examples/resource/amazon_ec2.py",
    "schemas"          : {"ec2"         : "Amacon EC2 key/secret",
                          "ec2_keypair" : "Amacon EC2 keypair name"}
}

# ------------------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA

_ADAPTOR_INFO          = {
    "name"             : _ADAPTOR_NAME,
    "version"          : "v0.1",
    "schemas"          : _ADAPTOR_SCHEMAS,
    "cpis"             : [
                             {
                                 "type"  : "radical.saga.Context",
                                 "class" : "EC2Keypair"
                             },
                             {
                                 "type"  : "radical.saga.resource.Manager",
                                 "class" : "EC2ResourceManager"
                             },
                             {
                                 "type"  : "radical.saga.resource.Compute",
                                 "class" : "EC2ResourceCompute"
                             },
                         ]
}


###############################################################################
# The adaptor class
class Adaptor (a_base.Base):
    """

    **Known Limitations, Notes**

    1) EC2 reports the VM instance to be 'Running' when it starts booting -- at
    that point the ssh login is not yet functional, and job service instance
    creation will thus fail.  I don't see a simple way to inspect the internal
    state (apart from trial/error which is not implemented), so the application
    needs to cater for that, for example by re-trying to connect.

    2) use `export LIBCLOUD_DEBUG=/dev/stderr` for debugging

    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self) :

        a_base.Base.__init__(self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        # for id parsing
        self.id_re = re.compile ('^\[(.*)\]-\[(.*?)\]$')

        self._default_contexts = list()


    # --------------------------------------------------------------------------
    #
    def sanity_check (self) :

        try :
            from ...utils import misc as rsutm

            # get the libclound modules.  Note that the non-empty fromlist
            # forces Python to include the actually specified module, not only
            # the top level libcloud.  Oh Python...
            self.lc   = __import__ ('libcloud',                   fromlist=[''])
            self.lcct = __import__ ('libcloud.compute.types',     fromlist=[''])
            self.lccp = __import__ ('libcloud.compute.providers', fromlist=[''])

            # we only tested against libcloud 0.12.4, so we require that version
            # for now, at least
            if  rsutm.normalize_version (self.lc.__version__) < \
                rsutm.normalize_version ('0.12.4')       :
                raise rse.NoSuccess ("Libcloud version >=0.12.4 required")


            # 0.12.4 does not support keypair footprint inspection, so we cannot
            # create ssh keys from keypair names.  We check for that feature
            # here, and issue a warning otherwise
            # for now, at least
            if  rsutm.normalize_version (self.lc.__version__) < \
                rsutm.normalize_version ('0.12.5')       :
                self._logger.warning ("Libcloud version does not allow keypair "
                                      "inspection -- cannot associate ssh keys "
                                      "with ec2 keypairs")
                self.lc_has_footprint = False
            else :
                self.lc_has_footprint = True

            # we pick EC2 environment parameters for default settings
            self._EC2_URL        = os.environ.get ('EC2_URL'       , "")
            self._EC2_SECRET_KEY = os.environ.get ('EC2_ACCESS_KEY', "")
            self._EC2_ACCESS_KEY = os.environ.get ('EC2_SECRET_KEY', "")


        except Exception as e :
            self._logger.warning ("Could not load libcloud module, "
                                  "disable EC2 resource adaptor")
            self._logger.warning (str(e))
            raise rse.NoSuccess ("Cannot load libcloud") from e


    # --------------------------------------------------------------------------
    #
    def _get_default_contexts (self) :

        if  None is self._default_contexts :

            self._default_contexts = list()

            # no default keypair in ec2 -- but lets see if we have default access
            # information
            if  not self._EC2_URL        and \
                not self._EC2_ACCESS_KEY and \
                not self._EC2_SECRET_KEY :
                # no default access info...
                return []

            # ok, lets pick up a default context from the EC2 default env vars
            ctx = api_context.Context ('ec2')

            if self._EC2_URL        : ctx.server   = self._EC2_URL
            if self._EC2_ACCESS_KEY : ctx.user_id  = self._EC2_ACCESS_KEY
            if self._EC2_SECRET_KEY : ctx.user_key = self._EC2_SECRET_KEY

            self._default_contexts.append (ctx)

        return self._default_contexts


    # --------------------------------------------------------------------------
    #
    def parse_id (self, id) :
        # split the id '[manager-url]-[resource-url]' in its parts, and return
        # them.

        match = self.id_re.match (id)

        if  not match or len (match.groups()) != 2 :
            raise rse.BadParameter ("Cannot parse resource id '%s'" % id)

        return (rs_url.Url (match.group(1)), str (match.group (2)))


    # --------------------------------------------------------------------------
    #
    def connect (self, session=None, url=None) :

        if  not session :
            session = rs_session.Session (default=True)

        ctx_id  = None
        ctx_key = None
        ctx_url = None
        error   = None

        ec2_url = None

        for ctx in session.contexts :

            if  ctx.type.lower () == 'ec2' :
                ctx_id  = ctx.user_id
                ctx_key = ctx.user_key
                ctx_url = rs_url.Url (ctx.server)

                if  not ctx_url or not str(ctx_url) :
                    ctx_url = rs_url.Url (self._EC2_URL)

                if  url :
                    ec2_url = url
                else :
                    ec2_url = ctx_url

                # ec2_url may still be empty!

                driver = None
                if  not ec2_url                     or \
                    ec2_url.scheme  == 'ec2'        or \
                    ec2_url.scheme  == 'aws'        or \
                    ec2_url.scheme  == 'amazon'        :
                    driver  = self.lccp.get_driver (self.lcct.Provider.EC2)
                    backend = 'aws'
                elif ec2_url.scheme == 'eucalyptus' or \
                    ec2_url.scheme  == 'euca'       or \
                    ec2_url.scheme  == 'http'       or \
                    ec2_url.scheme  == 'https'      or \
                    ec2_url.scheme  == 'openstack'     :
                    driver = self.lccp.get_driver(self.lcct.Provider.EUCALYPTUS)
                    backend = 'euca'
                else :
                    error = "URL schema not supported by adaptor(%s)" % ec2_url
                    next


                if  backend  == 'aws' :
                    self._logger.debug ("aws backend")
                    conn = driver (ctx_id, ctx_key)
                    # do we need URL details for different availability zones?

                elif backend == 'euca' :
                    self._logger.debug ("eucalyptus backend")
                    conn = driver (ctx_id, ctx_key,
                                 # secure = True, # how do we know?
                                   host   = ctx_url.host,
                                   port   = ctx_url.port,
                                   path   = ctx_url.path)
                else :
                    error = "only EC2 supported (not %s)" % ec2_url
                    next

                return conn, backend

        # no luck, didn't get a valid connection...
        if  error :
            raise rse.BadParameter (error)

        # no particular context failed -- raise generic exception
        raise rse.BadParameter("no valid EC2 credentials found (ec2 url='%s')"
                              % ec2_url)


###############################################################################
#
class EC2Keypair (cpi_context.Context) :

    """
    This context points to an EC2 keypair which is used to contextualize VM
    instances.

    The context can be used in two ways, depending on the specified keys:

    Version 1: reference an existing (uploaded) keypair:
      - `Token`  : name of keypair to be used  (required)
      - `UserID` : username on VM instance     (optional, default 'root')
      - `Server` : authentication server host  (optional, default for Amazon)

    A 'DoesNotExist' exception will be raised if the referenced keypair does not
    exist, and the context will not be added to the session.  An attempt to
    connect to a VM with an invalid keypair (i.e.  a keypair not registered upon
    VM creation), or with an invalid user id, will result in an
    'AuthorizationDenied' exception.


    Version 2: create (upload) a new keypair, and the use it
      - `Token`  : name of keypair to create   (required)
      - `UserKey`: private or public  ssh key  (required)
      - `UserID` : username on VM instance     (optional, default 'root')
      - `Server` : authentication server host  (optional, default for Amazon)

    When used in this version, the adaptor will attempt to create an EC2 keypair
    with the given name (`Token`), by uploading the public ssh key.  On success,
    the `UserKey` attribute will then be removed from the context, to avoid
    repeated uploads on re-use, and the context will behave as in Version 1.  If
    a keypair with the given name already exists, an 'AlreadyExists' exception
    is raised.  All other errors with result in a `NoSuccess` exception.  If any
    error occurs, the context will not be added to the session.

    The `UserKey` attribute can point to either the public or private key of the
    ssh keypair -- the RADICAL-SAGA implementation will internally complete the
    respective other key (public key file names are expected to be derived from
    the private key, by appending the suffix `.pub`).

    *Note* that `Token` is not a standard :class:`saga.Context` attribute -- at
    this point, there seems to exist no suitable attribute for that
    functionality.  The `Token` attribute may be officially included into the
    list of context attributes at some point.


    Known Limitations:

    1) For a given EC2 keypair name, we should fetch the respective key
    footprint with `conn.ex_describe_keypairs('self.api.target')`, then sift
    through all public ssh keys we can find, and see if one matches that
    footprint.  If one does, we should add that respective ssh key to the
    session, so that it can be used for host access authentication.

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

    """


    # --------------------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        _cpi_base = super  (EC2Keypair, self)
        _cpi_base.__init__ (api, adaptor)

        self.lcct = self._adaptor.lcct
        self.lccp = self._adaptor.lccp


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, type) :

        if  not type.lower () == 'ec2'         and \
            not type.lower () == 'ec2_keypair' :
            raise rse.BadParameter("ec2 adaptor only handles 'ec2' and "
                                   "'ec2_keypair' contexts")

        self._type = type

        return self


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def _initialize (self, session) :

        # nothing to do for simple ec2 id/secret containers
        # FIXME: we could in principle validate validity...
        if  self._type.lower () == 'ec2' :
            return

        # we first attempt to create an ssh context from the keypair
        # context -- this will take care of all eventual key checks etc.
        ssh_context = api_context.Context ('ssh')

        ssh_context.user_id   = self._api ().user_id
        ssh_context.user_key  = self._api ().user_key
        ssh_context.user_cert = self._api ().user_cert
        ssh_context.user_pass = self._api ().user_pass

        # contexts are verified on session.add_context -- to force that
        # verification we use a temporary session.  This will check if the ssh
        # key exists, etc.
        tmp_session = rs_session.Session (default=False)
        tmp_session.add_context (ssh_context)


        # we have an ec2_keypair context.  We need to find an ec2  context in
        # the session though which we can use to talk to the backend
        self.ec2_id  = None
        self.ec2_key = None

        for ctx in session.contexts :
            if  ctx.type.lower () == 'ec2' :
                self.ec2_id  = ctx.user_id
                self.ec2_key = ctx.user_key
                break  # only need one of those...

        if  not self.ec2_id or not self.ec2_key :
            raise rse.AuthenticationFailed \
                  ("no ec2 context -- cannot initialize ec2_keypair")

        if  not self._api ().token :
            raise rse.BadParameter \
                  ("`ec2_keypair` context must specify keypair name as `token`")

      # # valid context, connect to backend
      # # FIXME: use 'Server' if defined
      # conn, backend = self._adaptor.connect (session)
      #
      # # check if given keypair exists.  We only can do that by inspecting it,
      # # and capturing an eventual exception.
      # keypair = None
      # token   = self._api ().token
      # ssh_id  = ssh_context.user_id
      # key     = ssh_context.user_key
      # cert    = ssh_context.user_cert
      # keypass = ssh_context.user_pass
      #
      # # With theese information, attempt to verify or upload the keypair.
      # upload  = False
      # try:
      #     # try to find it
      #     print('describe %s' % token)
      #     keypair = conn.ex_describe_keypairs (token)
      #
      #     if  not keypair               or \
      #         not 'keyName' in keypair  or \
      #         not keypair['keyName']       :
      #
      #         self._logger.info ("keypair check nok: %s" % (keypair))
      #         upload = True
      #
      #     else :
      #         self._logger.info ("keypair check ok: %s" % (keypair))
      #
      # except Exception as e :
      #
      #     self._logger.error ("keypair check : %s" % (e))
      #
      #     # FIXME: actually, key management only seems to work for AWS/EC2
      #     # proper (ex_describe_keypairs is documented as EC2 only by
      #     # libcloud), so we only raise a warning on errors for other
      #     # backends.
      #
      #     if  backend == 'aws' :
      #
      #         if str(e).startswith ("InvalidKeyPair.NotFound") :
      #
      #             if  not key :
      #                 raise BadParameter \
      #                       ("'ec2_keypair' not found: %s" % e)
      #
      #             # keypair not found, but we have a key and can register it
      #             upload = True
      #
      #         else :
      #             raise BadParameter \
      #                   ("'ec2_keypair' invalid: %s" % e)
      #
      #
      # # upload keypair if we did not find it, and have something to upload.
      # if  upload and key :
      #
      #     self._logger.info ("import new keypair %s : %s" % (token, key))
      #
      #     try :
      #         keypair = conn.ex_import_keypair (token, key)
      #         self._logger.info ("keypair upload gave %s" % keypair)
      #
      #
      #     except Exception as e :
      #         raise BadParameter \
      #               ("'ec2_keypair' not imported: %s" % e)
      #
      #     # import worked -- we don't need to import again, so unset the
      #     # user_key attribute
      #     self._api ().user_key  = None
      #     self._api ().user_cert = None
      #
      #
      # # did not find it, and have nothing to upload?!
      # elif upload and not key :
      #     self._logger.error ("no 'UserKey' for ec2_keypair '%s'" % token)
      #
      # # keypair found all right
      # else :
      #     self._logger.info ("validated ec2_keypair '%s'" % token)


        # we add the thusly derived ssh context to the session which originally
        # contained our ec2_keypair context.  We do that even for failed
        # uploads, in the hope that some out-of-band setup kicks in.
        session.add_context (ssh_context)


###############################################################################
#
class EC2ResourceManager (cpi_resource.Manager) :
    """

    **EC2_URLs:**


    AWS Generic access point           https://ec2.amazonaws.com/
    AWS US East (Northern Virginia)    https://ec2.us-east-1.amazonaws.com/
    AWS US West (Oregon)               https://ec2.us-west-2.amazonaws.com/
    AWS US West (Northern California)  https://ec2.us-west-1.amazonaws.com/
    AWS EU (Ireland)                   https://ec2.eu-west-1.amazonaws.com/
    AWS Asia Pacific (Singapore)       https://ec2.ap-southeast-1.amazonaws.com/
    AWS Asia Pacific (Tokyo)           https://ec2.ap-northeast-1.amazonaws.com/
    AWS South America (Sao Paulo)      https://ec2.sa-east-1.amazonaws.com/

    OSDC                               euca://api.opensciencedatacloud.org:8773\
                                                        /sullivan/services/Cloud

    **Known Limitations**

    1) the EC2 backend reports a VM to be in `ACTIVE` state as soon as it begins
       to boot up (From that point in time on, the VM indeed consumes resources,
       and is billed against the user's account).  The VM is at that point,
       however, not yet reachable, as the boot process needs to be completed
       bevore the ssh daemon (or any other service) can operate correctly.
       Furthermore, the VM's IP address may not yet be registered in DNS,
       further limiting reachability of the machine.  Finally, it seems that
       also the VM contextualization is at this point not completed, so that for
       example an ssh connection may fail due to unregistered keys.

       There does not seem to exist and universal way to wait for the boot,
       registration and contextualization completion.  Retrying to connect is
       one option, but (a) the connection mechanism depends on the VM's OS image
       configuration, and (b) a connection failure due to incomplete system
       startup is indistinguishable from any 'real' system error.

       For the time being it is left to the application level on how to handle
       that problem, and this adaptor will simply report the state as returned
       by EC2.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (EC2ResourceManager, self)
        self._cpi_base.__init__ (api, adaptor)

        self.lcct = self._adaptor.lcct
        self.lccp = self._adaptor.lccp


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, url, session) :

        self.url     = rs_url.Url (url)  # deep copy
        self.session = session

        # internale (cached) registry of available resources
        self.templates         = []
        self.templates_dict    = {}
        self.images            = []
        self.images_dict       = {}
        self.access            = {}
        self.access[c.COMPUTE] = []
        self.access[c.STORAGE] = []
        self.access[c.ANY]     = []

        self.conn, self.backend = self._adaptor.connect (session, url)

        self.templates = []
        self.images    = []

        # FIXME: we could pre-fetch existing resources right now...


    # --------------------------------------------------------------------------
    #
    def _refresh_templates (self, pattern=None) :

        self.templates      = []
        self.templates_dict = {}

        for template in self.conn.list_sizes (pattern) :

            self.templates_dict   [template.name] = template
            self.templates.append (template.name)


    # --------------------------------------------------------------------------
    #
    def _refresh_images (self, uid=None) :

        self.images      = []
        self.images_dict = {}

        if uid:
            pattern = [uid]
        else:
            pattern = None

        for image in self.conn.list_images (ex_image_ids=pattern) :

            if  image.id.startswith ('ami-') :

                self.images_dict   [image.id] = image
                self.images.append (image.id)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def acquire (self, rd) :

        if  not self.conn :
            raise rse.IncorrectState ("not connected to backend")

        if  rd.rtype != c.COMPUTE:
            raise rse.BadParameter ("can only acquire compute resources.")


        # check if a any 'ec2_keypair' context is known.  If so, use its
        # 'keypair'
        # attribute as keypair name for node creation
        token = ''
        for context in self.session.contexts  :
            if  context.type.lower () == 'ec2_keypair' :
                token = context.token
                self._logger.info ("using '%s' as ec2 keypair" % token)

        resource_info = None

        # check that only supported attributes are provided
        for attribute in rd.list_attributes():
            if attribute not in _ADAPTOR_CAPABILITIES["rdes_attributes"]:
                msg = "'resource.Description.%s' unsupported by this adaptor" \
                                                                     % attribute
                raise rse.BadParameter._log (self._logger, msg)


        # we only support template defined instances right now
        # FIXME: should be able to select suitable template from given
        # resource attributes
        if  not rd.template :
            raise rse.BadParameter ("no 'template' in resource description")

        # we also need an OS image
        if  not rd.image :
            raise rse.BadParameter ("no 'image' in resource description")

        # and we don't support any other attribute right now
        if  rd.dynamic      or rd.start        or \
            rd.end          or rd.duration     or \
            rd.machine_os   or rd.machine_arch or \
            rd.access       or rd.memory       :
            raise rse.BadParameter ("resource descriptions only supports "
                                    "'template' and 'image' attributes "
                                    "right now")

        try :

            # make sure template and image are valid, and get handles
            if  rd.template not in self.templates_dict :
                self._refresh_templates (rd.template)

            if  rd.image not in self.images_dict :
                self._refresh_images (uid=rd.image)


            # FIXME: interpret / verify size

            # user name as id tag
            import getpass
            cid = getpass.getuser()
            _c   = self.conn

            # create/use the saga-sg security group which allows ssh access
            try:
                ret = _c.ex_create_security_group('saga-sg','SAGA', None)
                ret = _c.ex_get_security_groups(group_names=['saga-sg'])
                gid = ret[0].id
                ret = _c.ex_authorize_security_group_ingress(gid, 22, 22,
                                                         cidr_ips=['0.0.0.0/0'])
                ret = _c.ex_authorize_security_group_egress (gid, 22, 22,
                                                         cidr_ips=['0.0.0.0/0'])

            except Exception as e:
                # lets hope this was a race and the group now exists...
                pass

            # it should be safe to create the VM instance now
            node = _c.create_node(name='radical.saga.resource.Compute.%s' % cid,
                                  size=self.templates_dict[rd.template],
                                  image=self.images_dict[rd.image],
                                  ex_keyname=token,
                                  ex_security_groups=['saga-sg'])

            resource_info = {'backend'              : self.backend   ,
                             'resource'             : node           ,
                             'resource_type'        : rd.rtype       ,
                             'resource_description' : rd             ,
                             'resource_manager'     : self.get_api (),
                             'resource_manager_url' : self.url       ,
                             'resource_schema'      : self.url.schema,
                             'connection'           : self.conn      }

        except Exception as e :
            # FIXME: translate errors more sensibly
            raise rse.NoSuccess ("Failed with %s" % e) from e

        if  resource_info :
            if  rd.rtype == c.COMPUTE :
                return api_resource.Compute(_adaptor=self._adaptor,
                                            _adaptor_state=resource_info)

        raise rse.NoSuccess ("Could not acquire requested resource")


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def acquire_by_id(self, rid):

        if  not self.conn :
            raise rse.IncorrectState ("not connected to backend")


        try :

            manager_url, rid_s = self._adaptor.parse_id (str(rid))

            # FIXME: interpret / verify size
            nodes  = self.conn.list_nodes (ex_node_ids=[rid_s])

            if  len (nodes) < 1 :
                raise rse.BadParameter ("Cannot find resource '%s'" % rid_s)
            if  len (nodes) > 1 :
                raise rse.BadParameter ("Cannot identify resource '%s'" % rid_s)

            node = nodes[0]

            resource_info = {'backend'              : self.backend   ,
                             'resource'             : node           ,
                             'resource_type'        : c.COMPUTE      ,
                             'resource_description' : None           ,
                             'resource_manager'     : self.get_api (),
                             'resource_manager_url' : self.url       ,
                             'resource_schema'      : self.url.schema,
                             'connection'           : self.conn      }

        except Exception as e :
            # FIXME: translate errors more sensibly
            raise rse.NoSuccess ("Failed with %s" % e) from e

        return api_resource.Compute(_adaptor=self._adaptor,
                                    _adaptor_state=resource_info)


    # --------------------------------------------------------------------------
    @SYNC_CALL
    def get_url (self) :

        return self.url


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def list (self, rtype):

        if  not self.conn :
            raise rse.IncorrectState ("not connected to backend")


        ret = []

        try :

            for node in self.conn.list_nodes () :
                ret.append ("[%s]-[%s]" % (self.url, node.id))

        except Exception as e :
            # FIXME: translate errors more sensibly
            raise rse.NoSuccess ("Failed with %s" % e) from e

        return ret


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def destroy (self, id):

        node = self.acquire (id)
        node.destroy ()


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def list_templates (self, rtype) :

        # we support only compute templates right now
        if  rtype and not (rtype & c.COMPUTE) :
            return []

        if not len (self.templates) :
            self._refresh_templates ()

        return self.templates


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_template (self, name) :

        # FIXME
        raise rse.BadParameter ("unknown template %s" % name)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def list_images (self, rtype) :

        # we support only compute images right now
        if  rtype and not (rtype & c.COMPUTE) :
            return []

        if not len (self.images) :
            self._refresh_images ()

        return self.images


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_image (self, img_id) :

        if  img_id not in self.images_dict:
            self._refresh_images (uid=img_id)

        if  img_id not in self.images_dict:
            raise rse.BadParameter ("unknown image %s" % img_id)

        descr = dict(self.images_dict[img_id].extra)

        if  'name' not in descr :
            descr['name'] = self.images_dict[img_id].name

        for key in list(descr.keys ()) :
            if  not descr[key] :
                descr.pop (key)

        return descr


###############################################################################
#
class EC2ResourceCompute (cpi_resource.Compute) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (EC2ResourceCompute, self)
        self._cpi_base.__init__ (api, adaptor)

        self.lcct = self._adaptor.lcct
        self.lccp = self._adaptor.lccp

        self.state       = c.NEW
        self.detail      = None
        self.rid         = None
        self.rtype       = None
        self.manager     = None
        self.manager_url = None


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_info, id, session):

        # eval id if given
        if  id :
            self.manager_url, self.rid = self._adaptor.parse_id (id)
            self.manager = api_resource.Manager(self.manager_url,
                                                session=session)


            if id not in self.manager.list(c.COMPUTE):

                raise rse.BadParameter ("resource '%s' not found" % id)

            cr = self.manager.acquire(id)

            self.backend     = cr._adaptor.backend
            self.resource    = cr._adaptor.resource
            self.rtype       = cr._adaptor.rtype
            self.descr       = cr._adaptor.descr
            self.manager     = cr._adaptor.manager
            self.manager_url = cr._adaptor.manager_url
            self.conn        = cr._adaptor.conn
            self.rid         = cr._adaptor.rid
            self.id          = cr._adaptor.id
            self.access      = cr._adaptor.access


        # no id -- grab info from adaptor_info
        elif adaptor_info :

            if  'backend'              not in adaptor_info or \
                'resource'             not in adaptor_info or \
                'resource_type'        not in adaptor_info or \
                'resource_description' not in adaptor_info or \
                'resource_manager'     not in adaptor_info or \
                'resource_manager_url' not in adaptor_info or \
                'connection'           not in adaptor_info    :
                raise rse.BadParameter(
                            "Cannot acquire resource, insufficient information")

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


        else :
            raise rse.BadParameter("Cannot acquire resource, no id/contact")


        # FIXME: we don't actually need new state, it should be fresh at
        # this point, right?!
        self._refresh_state ()


        return self.get_api ()


    # --------------------------------------------------------------------------
    #
    def _refresh_state (self) :

        if  self.state == c.EXPIRED :
            # no need to update, state is final
            return

        try :
            # NOTE: ex_node_ids is only supported by ec2
            nodes = self.conn.list_nodes (ex_node_ids=[self.rid])

            if  not len (nodes) :
                raise rse.IncorrectState ("resource '%s' disappeared")

            if  len (nodes) != 1 :
                self._logger.warning ("Cannot identify instance %s" % self.rid)

            self.resource = nodes[0]

            if  'status' in self.resource.extra :
                self.detail = self.resource.extra['status']

            # FIXME: move state translation to adaptor
            s = self.resource.state
            if   s == self.lcct.NodeState.RUNNING    : self.state = c.ACTIVE
            elif s == self.lcct.NodeState.REBOOTING  : self.state = c.PENDING
            elif s == self.lcct.NodeState.TERMINATED : self.state = c.EXPIRED
            elif s == self.lcct.NodeState.PENDING    : self.state = c.PENDING
            elif s == self.lcct.NodeState.UNKNOWN    : self.state = c.UNKNOWN
            else                                     : self.state = c.UNKNOWN

            if  self.state == c.UNKNOWN and self.detail == 'shutting-down' :
                self.state =  c.EXPIRED

        except Exception as e :
            self._logger.error ("Could not obtain resource state (%s): %s"
                             % (self.id, e))
            self.state  =  c.UNKNOWN

        if  self.state  == c.EXPIRED :
            self.access is None
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


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def reconfig (self):
        raise rse.NotImplemented ("This backend cannot reconfigre resources")


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def destroy (self):

        self.conn.destroy_node (self.resource)
        self.state  = c.EXPIRED
        self.detail = 'destroyed by user'


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def wait (self, state, timeout) :

        import time
        start = time.time ()

        while not self.state :
            self._refresh_state ()

        if  self.state == c.EXPIRED and \
            not  state  & c.EXPIRED :
                raise rse.IncorrectState ("resource is in final state (%s): %s"
                                        % (self.detail, self.id))

        while not (self.state & state):

            self._logger.info("wait   for resource state %s: %s"
                             % (state, self.state))

            if timeout > 0 :
                now = time.time ()

                if  (now - start > timeout) :
                    break

            elif timeout == 0 :
                break

            self._refresh_state ()

        self._logger.info("waited for resource state %s: %s"
                         % (state, self.state))
        return


# ------------------------------------------------------------------------------

