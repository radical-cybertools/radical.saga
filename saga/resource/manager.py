
""" """


import saga.base
import saga.async
import saga.url
import saga.session
import saga.exceptions         as se
import saga.constants          as sc
import saga.resource.constants as src
import saga.resource.description


# ------------------------------------------------------------------------------
# 
class Manager (saga.base.Base, saga.async.Async) :
    """
    In the context of SAGA-Python, a *ResourceManager* is a service which asserts
    control over a set of resources.  That manager can, on request, render
    control over subsets of those resources (resource slices) to an application.

    This :class:`Manager` class represents the contact point to such
    ResourceManager instances -- the application can thus aquire compute, data
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
    def list (self, type=None, ttype=None) :
        """ 
        :type  type: None or enum (COMPUTE | STORAGE | NETWORK)
        :param type: specifies a filter of resource types to list.

        List known resource instances (which can be aquired). 
        Returns a list of IDs.  
        """
        return self._adaptor.list (type, ttype)


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

        return self._adaptor.get_description (id, ttype)

    # --------------------------------------------------------------------------
    # 
    def list_templates (self, type=None, ttype=None) :
        """
        :type  type: None or enum (COMPUTE | STORAGE | NETWORK)
        :param type: specifies a filter of resource types to list.

        List template names available for the specified resource type(s).
        Returns a list of strings.
        """

        return self._adaptor.list_templates (type, ttype)


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

        return self._adaptor.get_template (name, ttype)


    # --------------------------------------------------------------------------
    # 
    def aquire (self, descr, ttype=None) :
        """
        :type  descr: :class:`Description`
        :param descr: specifies the resource to be acquired.

        Get a :class:`saga.resource.Resource` handle for the specified
        description.  Depending on the `TYPE` attribute in the description, the
        returned resource may be a :class:`saga.resource.Compute`,
        :class:`saga.resource.Storage` or :class:`saga.resource.Network`
        instance.  The returned resource will be in NEW, PENDING or ACTIVE
        state.
        """

        if  type(spec) == type(saga.url.Url) or \
            type(spec) == type(basestring)      :

            id = saga.url.Url (spec)
            
            return self._adaptor.aquire_by_id (id, ttype)

        else :

            descr = saga.resource.description.Description (spec)

            # make sure at least 'executable' is defined
            if descr.type is None:
                raise se.BadParameter ("No resource type defined in resource description")
    
            descr_copy = saga.resource.description.Description ()
            descr._attributes_deep_copy (descr_copy)

            return self._adaptor.acquire (descr_copy, ttype=ttype)


    # --------------------------------------------------------------------------
    # 
    def release (self, id, drain=False, ttype=None) :
        """
        :type  id   : string
        :param id   : identifies the resource to be released.

        :type  drain: bool
        :param drain: delay release until idle.

        This call requests to move a resource from any non-final state to the
        `CANCELED` state.  The `drain` flag will request the resource's release
        to be delayed until all active resource usage has completed -- during
        that draining time, the resource should not accept new usage requests.
        If the specific resource does not support the `drain` semantics,
        a :class:`saga.BadParameter` exception is raised if the flag is et to
        True.
        """

        return self._adaptor.release (id, drain, ttype)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

