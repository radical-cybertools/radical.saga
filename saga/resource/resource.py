
import saga.base
import saga.async
import saga.url
import saga.attributes
import saga.session
import saga.exceptions          as se
import saga.constants           as sc
import saga.resource.constants  as src


# ------------------------------------------------------------------------------
#
class Resource (saga.base.Base, saga.attributes.Attributes, saga.async.Async) :
    """
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, id=None, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        """
        reconnect to an existing resource
        
        :param id: id of the resource
        :type  id: :class:`saga.Url`
        
        :param session: SAGA session to be used
        :type  session: :class:`saga.Session`
        """

        # set attribute interface properties

        import saga.attributes as sa

        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface

        self._attributes_register  (src.ID          , None, sa.ENUM,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (src.TYPE        , None, sa.ENUM,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (src.STATE       , None, sa.ENUM,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (src.STATE_DETAIL, None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (src.MANAGER     , None, sa.URL,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (src.DESCRIPTION , None, sa.ANY,    sa.SCALAR, sa.WRITEABLE)

        self._attributes_set_enums (src.STATE, [src.UNKNOWN ,
                                                src.PENDING ,
                                                src.ACTIVE  ,
                                                src.DRAINING,
                                                src.RUNNING ,
                                                src.CLOSED  ,
                                                src.EXPIRED ,
                                                src.FAILED  ,
                                                src.FINAL   ])

        self._attributes_set_enums (src.TYPE,  [src.COMPUTE ,
                                                src.STORAGE ,
                                                src.NETWORK ])


        self._attributes_set_getter (src.ID          , self.get_id          )
        self._attributes_set_getter (src.TYPE        , self.get_type        )
        self._attributes_set_getter (src.STATE       , self.get_state       )
        self._attributes_set_getter (src.STATE_DETAIL, self.get_state_detail)
        self._attributes_set_getter (src.MANAGER     , self.get_manager     )
        self._attributes_set_getter (src.DESCRIPTION , self.get_description )


        # we need the ID to be an URL, as we don't have a scheme otherwise,
        # which means we can't select an adaptor.  Duh! :-/

        # param checks
        url    = saga.url.Url (id)
        scheme = url.scheme.lower ()

        if not session :
            session = saga.session.Session (default=True)

        saga.base.Base.__init__ (self, scheme, _adaptor, _adaptor_state, 
                                 url, session, ttype=_ttype)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def create (cls, id=None, session=None, ttype=sc.SYNC) :
        """ 
        """

        return cls (id, session, _ttype=ttype)._init_task


    # --------------------------------------------------------------------------
    #
    def reconfig (self, descr, ttype=None) :

        return self._adaptor.reconfig (descr, ttype)


    # --------------------------------------------------------------------------
    #
    def release (self, drain=False, ttype=None) :

        return self._adaptor.release (drain, ttype)


    # --------------------------------------------------------------------------
    #
    def wait (self, state=src.FINAL, timeout=-1.0, ttype=None) :
    
        return self._adaptor.wait (state, timeout, ttype)


    # --------------------------------------------------------------------------
    #
    def get_id           (self, ttype=None) : return self._adaptor.get_id            (ttype)
    def get_type         (self, ttype=None) : return self._adaptor.get_type          (ttype)
    def get_state        (self, ttype=None) : return self._adaptor.get_state         (ttype)
    def get_state_detail (self, ttype=None) : return self._adaptor.get_state_detail  (ttype)
    def get_manager      (self, ttype=None) : return self._adaptor.get_manager       (ttype)
    def get_description  (self, ttype=None) : return self._adaptor.get_description   (ttype)
# 
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
class Compute (Resource) :

    def __init__ (self, id=None, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        
        self._resrc = super  (Compute, self)
        self._resrc.__init__ (id, session, _adaptor, _adaptor_state, _ttype)

        if  self.type != src.COMPUTE :
            raise se.BadParameter ("Cannot initialize Compute resource with %s id" \
                                % self.type)


# ------------------------------------------------------------------------------
#
class Storage (Resource) :

    def __init__ (self, id=None, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        
        self._resrc = super  (Storage, self)
        self._resrc.__init__ (id, session, _adaptor, _adaptor_state, _ttype)

        if  self.type != src.STORAGE :
            raise se.BadParameter ("Cannot initialize Storage resource with %s id" \
                                % self.type)


# ------------------------------------------------------------------------------
#
class Network (Resource) :

    def __init__ (self, id=None, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        
        self._resrc = super  (Network, self)
        self._resrc.__init__ (id, session, _adaptor, _adaptor_state, _ttype)

        if  self.type != src.NETWORK :
            raise se.BadParameter ("Cannot initialize Network resource with %s id" \
                                % self.type)
# 
# ------------------------------------------------------------------------------


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

