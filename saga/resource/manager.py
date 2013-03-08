
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
    '''
    '''
    
    # --------------------------------------------------------------------------
    # 
    def __init__ (self, url_in, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        '''
        '''

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
        """

        return cls (url_in, session, _ttype=ttype)._init_task


    # --------------------------------------------------------------------------
    # 
    def list (self, type=None, ttype=None) :
        """ 
        list known pilot/vm/ar instances etc. (which can be aquired). Returns
        a list of IDs.
        """
        return self._adaptor.list (type, ttype)


    # --------------------------------------------------------------------------
    # 
    def get_description (self, id, ttype=None) :
        # see drmaav2::machine_info?  Add GLUE inspection as
        # read-only attribs?  link to SD or ISN?

        return self._adaptor.get_description (id, ttype)

    # --------------------------------------------------------------------------
    # 
    def list_templates (self, type=None, ttype=None) :
        # list available templates

        return self._adaptor.list_templates (type, ttype)


    # --------------------------------------------------------------------------
    # 
    def get_template (self, name, ttype=None) :
        # human readable description of template

        return self._adaptor.get_template (name, ttype)


    # --------------------------------------------------------------------------
    # 
    def aquire (self, spec, ttype=None) :

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

        return self._adaptor.release (id, drain, ttype)

    

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

