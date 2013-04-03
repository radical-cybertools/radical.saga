
""" """


import saga.base
import saga.async
import saga.url
import saga.session
import saga.exceptions  as se
import saga.constants   as sc
import constants        as const
import description      as descr


# ------------------------------------------------------------------------------
# 
class Manager (saga.base.Base, saga.async.Async) :
    """
    In the context of SAGA-Python, a *ResourceManager* is a service which asserts
    control over a set of resources.  That manager can, on request, render
    control over subsets of those resources (resource slices) to an application.

    This :class:`Manager` class represents the contact point to such
    ResourceManager instances -- the application can thus acquire compute, data
    or network resources, according to some resource specification, for a bound
    or unbound amount of time. 
    """
    
    # --------------------------------------------------------------------------
    # 
    def __init__ (self, url_in, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        """
        :type  url_in: :class:`saga.Url`
        :param url_in: the contact point of the resource manager service.
        """

        # param checks
        url    = saga.url.Url (url_in)
        scheme = url.scheme.lower ()

        if not session :
            session = saga.session.Session (default=True)

        saga.base.Base.__init__ (self, scheme, _adaptor, _adaptor_state, 
                                 url, session, ttype=_ttype)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def create (cls, url_in=None, session=None, ttype=sc.SYNC) :
        """ 
        This is the asynchronous class constructor, returning
        a :class:`saga:Task` instance.  For details on the accepted parameters,
        please see the description of :func:`__init__`.
        """

        return cls (url_in, session, _ttype=ttype)._init_task


    # --------------------------------------------------------------------------
    # 
    def list (self, rtype=None, ttype=None) :
        """ 
        :type  rtype: None or enum (COMPUTE | STORAGE | NETWORK)
        :param rtype: specifies a filter of resource types to list.

        List known resource instances (which can be acquired). 
        Returns a list of IDs.  
        """
        return self._adaptor.list (rtype, ttype=ttype)


    # --------------------------------------------------------------------------
    # 
    def get_description (self, id, ttype=None) :
        """ 
        :type  id: string
        :param id: identifies the resource to be described.

        Get a resource :class:`Description` for the specified resource.

        NOTE: see drmaav2::machine_info?  Add GLUE inspection as
        read-only attribs?  link to SD or ISN?
        """

        return self._adaptor.get_description (id, ttype=ttype)

    # --------------------------------------------------------------------------
    # 
    def list_templates (self, rtype=None, ttype=None) :
        """
        :type  rtype: None or enum (COMPUTE | STORAGE | NETWORK)
        :param rtype: specifies a filter of resource types to list.

        List template names available for the specified resource type(s).
        Returns a list of strings.
        """

        return self._adaptor.list_templates (rtype, ttype=ttype)


    # --------------------------------------------------------------------------
    # 
    def get_template (self, name, ttype=None) :
        """
        :type  name: string
        :param name: specifies the template to be queried for a description.

        Get a resource :class:`Description` for the specified template.

        The returned resource description instance may not have all attributes
        filled, and may in fact not sufficiently complete to allow for
        successful resource acquisition.  The only guaranteed attribute in the
        returned description is `TEMPLATE`, containing the very template id
        specified in the call parameters.
        """

        return self._adaptor.get_template (name, ttype=ttype)


    # --------------------------------------------------------------------------
    # 
    def acquire (self, spec, ttype=None) :
        """
        :type  descr: :class:`Description`
        :param descr: specifies the resource to be acquired.

        Get a :class:`saga.resource.Resource` handle for the specified
        description.  Depending on the `RTYPE` attribute in the description, the
        returned resource may be a :class:`saga.resource.Compute`,
        :class:`saga.resource.Storage` or :class:`saga.resource.Network`
        instance.  The returned resource will be in NEW, PENDING or ACTIVE
        state.
        """

        if  type(spec) == type(saga.url.Url) or \
            type(spec) == type(basestring)      :

            id = saga.url.Url (spec)
            
            return self._adaptor.acquire_by_id (id, ttype=ttype)

        else :

            descr = saga.resource.Description (spec)

            # make sure at least 'executable' is defined
            if descr.rtype is None:
                raise se.BadParameter ("No resource type defined in resource description")
    
            descr_copy = saga.resource.Description ()
            descr._attributes_deep_copy (descr_copy)

            return self._adaptor.acquire (descr_copy, ttype=ttype)


    # --------------------------------------------------------------------------
    # 
    def release (self, id, ttype=None) :
        """
        :type  id   : string
        :param id   : identifies the resource to be released.

        This call requests to move a resource from any non-final state to the
        `CANCELED` state.  
        """

        return self._adaptor.release (id, ttype=ttype)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

