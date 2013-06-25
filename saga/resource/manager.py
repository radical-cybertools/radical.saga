
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"

import saga.utils.signatures    as sus
import saga.adaptors.base       as sab
import saga.async               as async
import saga.task                as st
import saga.base                as sb
import saga.session             as ss
import saga.exceptions          as se
import saga.attributes          as sa
import saga.constants           as sc
import saga.url                 as surl
import constants                as const
import description              as descr
import resource                 as resrc

from   saga.resource.constants  import *
from   saga.constants           import SYNC, ASYNC, TASK


# ------------------------------------------------------------------------------
# 
class Manager (sb.Base, async.Async) :
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
    @sus.takes   ('Manager', 
                  sus.optional (basestring, surl.Url), 
                  sus.optional (ss.Session),
                  sus.optional (sab.Base), 
                  sus.optional (dict), 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (sus.nothing)
    def __init__ (self, url_in=None, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        """
        :type  url_in: :class:`saga.Url`
        :param url_in: the contact point of the resource manager service.
        """

        # param checks
        url    = surl.Url (url_in)
        scheme = url.scheme.lower ()

        if not session :
            session = ss.Session (default=True)

        self._base = super  (Manager, self)
        self._base.__init__ (scheme, _adaptor, _adaptor_state, 
                             url, session, ttype=_ttype)


    # --------------------------------------------------------------------------
    #
    @classmethod
    @sus.takes   ('Manager', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (ss.Session),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (st.Task)
    def create   (cls, url_in=None, session=None, ttype=sc.SYNC) :
        """ 
        This is the asynchronous class constructor, returning
        a :class:`saga:Task` instance.  For details on the accepted parameters,
        please see the description of :func:`__init__`.
        """

        return cls (url_in, session, _ttype=ttype)._init_task


    # --------------------------------------------------------------------------
    # 
    @sus.takes   ('Manager', 
                  sus.optional (sus.one_of (COMPUTE, STORAGE, NETWORK)),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.list_of (basestring), st.Task))
    def list     (self, rtype=None, ttype=None) :
        """ 
        :type  rtype: None or enum (COMPUTE | STORAGE | NETWORK)
        :param rtype: specifies a filter of resource types to list.

        List known resource instances (which can be acquired). 
        Returns a list of IDs.  
        """
        return self._adaptor.list (rtype, ttype=ttype)


    # --------------------------------------------------------------------------
    # 
    @sus.takes   ('Manager', 
                  sus.optional (basestring),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((descr.Description, st.Task))
    def get_description (self, rid, ttype=None) :
        """ 
        :type  rid: string
        :param rid: identifies the resource to be described.

        Get a resource :class:`Description` for the specified resource.

        NOTE: see drmaav2::machine_info?  Add GLUE inspection as
        read-only attribs?  link to SD or ISN?

        NOTE: if rid is None, should we return a description of the managed
        resources?  
        """

        return self._adaptor.get_description (id, ttype=ttype)

    # --------------------------------------------------------------------------
    # 
    @sus.takes   ('Manager', 
                  sus.optional (sus.one_of (COMPUTE, STORAGE, NETWORK)),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.list_of (basestring), st.Task))
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
    @sus.takes   ('Manager', 
                  sus.optional (basestring),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((descr.Description, st.Task))
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
    @sus.takes   ('Manager', 
                  sus.optional (sus.one_of (COMPUTE, STORAGE, NETWORK)),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.list_of (basestring), st.Task))
    def list_images (self, rtype=None, ttype=None) :
        """
        :type  rtype: None or enum (COMPUTE | STORAGE | NETWORK)
        :param rtype: specifies a filter of resource types to list.

        List image names available for the specified resource type(s).
        Returns a list of strings.
        """

        return self._adaptor.list_images (rtype, ttype=ttype)


    # --------------------------------------------------------------------------
    # 
    @sus.takes   ('Manager', 
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((dict, st.Task))
    def get_image (self, name, ttype=None) :
        """
        :type  name: string
        :param name: specifies the image to be queried for a description.

        Get a description string for the specified image.
        """

        return self._adaptor.get_image (name, ttype=ttype)


    # --------------------------------------------------------------------------
    # 
    @sus.takes   ('Manager', 
                  (basestring, surl.Url, descr.Description),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((resrc.Resource, st.Task))
    def acquire  (self, spec, ttype=None) :
        """
        :type  spec: :class:`Description`
        :param spec: specifies the resource to be acquired.

        Get a :class:`saga.resource.Resource` handle for the specified
        description.  Depending on the `RTYPE` attribute in the description, the
        returned resource may be a :class:`saga.resource.Compute`,
        :class:`saga.resource.Storage` or :class:`saga.resource.Network`
        instance.  The returned resource will be in NEW, PENDING or ACTIVE
        state.
        """

        if  isinstance (spec, surl.Url) or \
            isinstance (spec, basestring)  :

            id = surl.Url (spec)
            
            return self._adaptor.acquire_by_id (id, ttype=ttype)

        else :

            # make sure at least 'executable' is defined
            if spec.rtype is None:
                raise se.BadParameter ("No resource type defined in resource description")
    
            spec_copy = descr.Description ()
            spec._attributes_deep_copy (spec_copy)

            return self._adaptor.acquire (spec_copy, ttype=ttype)


    # --------------------------------------------------------------------------
    # 
    @sus.takes   ('Manager', 
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
    def destroy  (self, id, ttype=None) :
        """
        :type  id   : string
        :param id   : identifies the resource to be released.

        This call requests to move a resource from any non-final state to the
        `CANCELED` state.  
        """

        return self._adaptor.destroy (id, ttype=ttype)

  # FIXME: add
  # templates = property (list_templates, get_template)    # dict {string : Description}
  # images    = property (list_images,    get_image)       # dict {string : dict}
  # resources = property (list,           get_description) # dict {string : Description}


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

