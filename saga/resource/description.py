
import saga.attributes
import saga.exceptions         as se
import saga.resource.constants as src

#-------------------------------------------------------------------------------
#
class Description (saga.attributes.Attributes) :
    """ 
    The resource description class. 

    Resource descriptions are used for two purposes:

      * an application can pass :class:`saga.resource.Description` instances to
        a :class:`saga.resource.Manager` instance, to request control over the
        resource slice described in the description; 

      * an application requests a resource's description for inspection of
      * resource properties.

    
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, d={}):

        # set attribute interface properties

        import saga.attributes as sa

        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface

        self._attributes_register  (src.TYPE        , None , sa.ENUM  , sa.SCALAR, sa.WRITEABLE) 
        self._attributes_register  (src.TEMPLATE    , None , sa.STRING, sa.SCALAR, sa.WRITEABLE) 
        self._attributes_register  (src.IMAGE       , None , sa.STRING, sa.SCALAR, sa.WRITEABLE) 
        self._attributes_register  (src.DYNAMIC     , False, sa.BOOL  , sa.SCALAR, sa.WRITEABLE) 
        self._attributes_register  (src.START       , None , sa.TIME  , sa.SCALAR, sa.WRITEABLE) 
        self._attributes_register  (src.END         , None , sa.TIME  , sa.SCALAR, sa.WRITEABLE) 
        self._attributes_register  (src.DURATION    , None , sa.TIME  , sa.SCALAR, sa.WRITEABLE) 
        self._attributes_register  (src.MACHINE_OS  , None , sa.STRING, sa.SCALAR, sa.WRITEABLE) 
        self._attributes_register  (src.MACHINE_ARCH, None , sa.STRING, sa.SCALAR, sa.WRITEABLE) 
        self._attributes_register  (src.SIZE        , 1    , sa.INT   , sa.SCALAR, sa.WRITEABLE) 
        self._attributes_register  (src.MEMORY      , None , sa.STRING, sa.SCALAR, sa.WRITEABLE) 
        self._attributes_register  (src.ACCESS      , None , sa.STRING, sa.SCALAR, sa.WRITEABLE) 

        self._attributes_set_enums (src.TYPE, [src.COMPUTE ,
                                               src.STORAGE ,
                                               src.NETWORK ])

        # FIXME: initialization should be done in Attributes...
        for key in d :
            self.set_attribute (key, d[key]) 


    # --------------------------------------------------------------------------
    #
    def __deepcopy__ (self, other) :
        return self.clone (other)

    # --------------------------------------------------------------------------
    #
    def clone (self, other=None) :
        """ 
        deep copy: unlike the default python assignment (copy object reference),
        a deep copy will create a new object instance with the same state --
        after a deep copy, a change on one instance will not affect the other.
        """

        # a job description only has attributes - so create a new instance,
        # clone the attribs, and done.
        if not other :
            other = saga.job.Description ()

        return self._attributes_deep_copy (other)


# ------------------------------------------------------------------------------
#
class ComputeDescription (Description) : 

    def __init__ (self, d={}) :
        
        if  src.TYPE in d and d[src.TYPE] != src.COMPUTE :
            raise se.BadParameter ("Cannot create ComputeResource with type '%s'" \
                                % d[src.TYPE])

        self._descr = super  (ComputeDescription, self)
        self._descr.__init__ (d)


# ------------------------------------------------------------------------------
#
class StorageDescription (Description) :

    def __init__ (self, d={}) :
        
        if  src.TYPE in d and d[src.TYPE] != src.STORAGE :
            raise se.BadParameter ("Cannot create StorageResource with type '%s'" \
                                % d[src.TYPE])

        self._descr = super  (StorageDescription, self)
        self._descr.__init__ (d)


# ------------------------------------------------------------------------------
#
class NetworkDescription (Description) :

    def __init__ (self, d={}) :
        
        if  src.TYPE in d and d[src.TYPE] != src.NETWORK :
            raise se.BadParameter ("Cannot create NetworkResource with type '%s'" \
                                % d[src.TYPE])

        self._descr = super  (NetworkDescription, self)
        self._descr.__init__ (d)



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

