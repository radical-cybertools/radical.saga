
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
class Resource (sb.Base, sa.Attributes, async.Async) :
    """
    A :class:`Resource` class instance represents a specific slice of resource
    which is, if in `RUNNING` state, under the applications control and ready to
    serve usage requests.  The type of accepted usage requests depends on the
    specific resource types (job execution for :class:`saga.resource.Compute`,
    data storage for :class:`saga.resource.Storage`, and network connectivity
    for :class:`saga.resource.Network`.  The exact mechanism how those usage
    requests are communicated are not part of the resource's class interface,
    but are instead served by other SAGA-Python classes -- typically those are
    :class:`saga.job.Service` for Compute resources, and
    :class:`saga.filesystem.Directory` for Storage resources (Network resources
    provide implicit connectivity, but do not have explicit, public entry points
    to request usage.

    The process of resource acquisition is performed by a *ResourceManager*,
    represented by a :class:`saga.resource.Manager` instance.  The semantics of
    the acquisition process is defined as the act of moving a slice (subset) of
    the resources managed by the resource manager under the control of the
    requesting application (i.e. under user control), to use as needed.  The
    type and property of the resource slice to be acquired and the time and
    duration over which the resource will be made available to the application
    are specified in a :class:`saga.resource.Description`, to be supplied when
    acquiring a resource.

    The exact backend semantics on *how* a resource slice is provisioned to the
    application is up to the resource manager backend -- this can be as simple
    as providing a job submission endpoint to a classic HPC resource, and as
    complex as instantiating a pilot job or pilot data container, or reserving
    a network fiber on demand, or instantiating a virtual machine -- the result
    will, from the application's perspective, indistinguishable: a resource
    slice is made available for the execution of usage requests (tasks,
    workload, jobs, ...).

    Resources are stateful: when acquired from a resource manager, they are
    typically in `NEW` state, and will become `ACTIVE` once they are provisioned
    to the application and can serve usage requests.  Some resources may  go
    through an intermediate state, `PENDING`, when they are about to become
    active at some point, and usage requests can already be submitted -- those
    usage requests will not be executed until the resources enters the `ACTIVE`
    state.  The resource can be release from application control in three
    different ways: they can be actively be released by the application, and
    will then enter the `CANCELED` state; they can internally cease to function
    and become unable to serve usage requests, represented by a `FAILED` state,
    and the resource manager can retract control from the application because
    the agreed time duration has passed -- this is represented by the `EXPIRED`
    state.
    """

    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Resource', 
                  sus.optional (basestring), 
                  sus.optional (ss.Session),
                  sus.optional (sab.Base), 
                  sus.optional (dict), 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (sus.nothing)
    def __init__ (self, id=None, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        """
        :param id: id of the resource
        :type  id: :class:`saga.Url`
        
        :param session: SAGA session to be used
        :type  session: :class:`saga.Session`

        Resource class instances are usually created by calling :func:`acquire`
        on the :class:`saga.resource.Manager` class.  Already acquired resources
        are identified by a string typed identifier.  This constructor accepts
        such an identifier to create another representation of the same
        resource.  As the resource itself is new newly acquired, it can be in
        any state.  In particular, it can be in a final state, and thus be
        unusable.  Further, the resource may already have expired or failed, and
        the information about it may have been purged -- in that case the id
        will not be valid any longer, and a :class:`saga.BadParameter` exception
        will be raised.

        The session parameter is interpreted exactly as the session parameter on
        the :class:`saga.resource.Manager` constructor.
        """

        # set attribute interface properties

        import saga.attributes as sa

        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface

        self._attributes_register  (const.ID          , None, sa.ENUM,   sa.SCALAR, sa.READONLY)
        self._attributes_register  (const.RTYPE       , None, sa.ENUM,   sa.SCALAR, sa.READONLY)
        self._attributes_register  (const.STATE       , None, sa.ENUM,   sa.SCALAR, sa.READONLY)
        self._attributes_register  (const.STATE_DETAIL, None, sa.STRING, sa.SCALAR, sa.READONLY)
        self._attributes_register  (const.ACCESS      , None, sa.URL,    sa.SCALAR, sa.READONLY)
        self._attributes_register  (const.MANAGER     , None, sa.URL,    sa.SCALAR, sa.READONLY)
        self._attributes_register  (const.DESCRIPTION , None, sa.ANY,    sa.SCALAR, sa.READONLY)

        self._attributes_set_enums (const.STATE, [const.UNKNOWN ,
                                                  const.PENDING ,
                                                  const.ACTIVE  ,
                                                  const.CANCELED,
                                                  const.EXPIRED ,
                                                  const.FAILED  ,
                                                  const.FINAL   ])

        self._attributes_set_enums (const.RTYPE, [const.COMPUTE ,
                                                  const.STORAGE ,
                                                  const.NETWORK ])


        self._attributes_set_getter (const.ID          , self.get_id          )
        self._attributes_set_getter (const.RTYPE       , self.get_rtype       )
        self._attributes_set_getter (const.STATE       , self.get_state       )
        self._attributes_set_getter (const.STATE_DETAIL, self.get_state_detail)
        self._attributes_set_getter (const.ACCESS      , self.get_access      )
        self._attributes_set_getter (const.MANAGER     , self.get_manager     )
        self._attributes_set_getter (const.DESCRIPTION , self.get_description )


        # we need the ID to be an URL, as we don't have a scheme otherwise,
        # which means we can't select an adaptor.  Duh! :-/

        # param checks
        scheme = None
        if  not id :
            if  not 'resource_schema' in _adaptor_state :
                raise se.BadParameter ("Cannot initialize resource without id" \
                                    % self.rtype)
            else :
                scheme = _adaptor_state['resource_schema']
        else :
            url    = surl.Url (id)
            scheme = url.scheme.lower ()


        if not session :

            session = ss.Session (default=True)

        self._base = super  (Resource, self)
        self._base.__init__ (scheme, _adaptor, _adaptor_state, 
                             id, session, ttype=_ttype)


    # --------------------------------------------------------------------------
    #
    @classmethod
    @sus.takes   ('resource', 
                  sus.optional (basestring), 
                  sus.optional (ss.Session),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (st.Task)
    def create   (cls, id=None, session=None, ttype=sc.SYNC) :
        """ 
        This is the asynchronous class constructor, returning
        a :class:`saga:Task` instance.  For details on the accepted parameters,
        please see the description of :func:`__init__`.
        """

        return cls (id, session, _ttype=ttype)._init_task


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Resource', 
                  descr.Description,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
    def reconfig (self, descr, ttype=None) :
        """
        A resource is acquired according to a resource description, i.e. to
        a specific set of attributes.  At some point in time, while the
        resource is running, the application requirements on the resource may
        have changed -- in that case, the application can request to change the
        resource attributes on the fly.

        This method cannot be used to change the type of the resource.  Backends
        may or may not support this operation -- if not,
        a :class:`saga.NotImplemented` exception is raised.  If yes, then the
        semantics of the method is equivalent to the semantics of the
        :func:`acquire` call on the :class:`saga.resource.Manager` class.
        """

        return self._adaptor.reconfig (descr, ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Resource', 
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
    def release (self, ttype=None) :
        """
        The semantics of this method is equivalent to the semantics of the
        :func:`release` call on the :class:`saga.resource.Manager` class.
        """

        return self._adaptor.release (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Resource', 
                  sus.optional (sus.one_of (UNKNOWN, NEW, PENDING, ACTIVE, DONE,
                                            FAILED, EXPIRED, CANCELED, FINAL)),
                  sus.optional (float),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
    def wait (self, state=const.FINAL, timeout=None, ttype=None) :
        """
        :type  state: state enum
        :param state: state to wait for

        :type  state: float
        :param state: time to block while waiting.

        The semantics of this method is equivalent to the semantics of the
        :func:`wait` call on the :class:`saga.resource.Manager` class.
        """
        # FIXME: should we also have a wait() on the manager?
        # FIXME: right now, we can not put a resource in a task container
    
        return self._adaptor.wait (state, timeout, ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Resource', sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, basestring, st.Task))
    def get_id   (self, ttype=None) : return self._adaptor.get_id            (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @sus.takes    ('Resource', sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns  ((sus.one_of (const.COMPUTE, 
                                const.STORAGE, 
                                const.NETWORK), st.Task))
    def get_rtype (self, ttype=None) : return self._adaptor.get_rtype         (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @sus.takes    ('Resource', sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns  ((sus.one_of (const.UNKNOWN ,
                                const.NEW     ,
                                const.PENDING ,
                                const.ACTIVE  ,
                                const.CANCELED,
                                const.EXPIRED ,
                                const.DONE    ,
                                const.FAILED  ,
                                const.FINAL   ), st.Task))
    def get_state (self, ttype=None) : return self._adaptor.get_state         (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Resource', sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, basestring, st.Task))
    def get_state_detail (self, ttype=None) : return self._adaptor.get_state_detail  (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @sus.takes     ('Resource', sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns   ((sus.nothing, basestring, st.Task))
    def get_access (self, ttype=None) : return self._adaptor.get_access        (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @sus.takes      ('Resource', sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns    ((basestring, st.Task))
    def get_manager (self, ttype=None) : return self._adaptor.get_manager       (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Resource', sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((descr.Description, st.Task))
    def get_description  (self, ttype=None) : return self._adaptor.get_description   (ttype=ttype)
# 
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
class Compute (Resource) :
    """
    A Compute resource is a resource which can execute compute jobs.  As such,
    the 'Access' attribute of the compute resource (a URL) can be used to create
    a :class:`saga.job.Service` instance to submit jobs to.
    """

    # --------------------------------------------------------------------------
    # FIXME: should 'ACCESS' be a list of URLs?  A VM could have an ssh *and*
    #        a gram endpoint...

    # @sus.takes   ('ComputeResource', 
    #               sus.optional (basestring), 
    #               sus.optional (ss.Session),
    #               sus.optional (sab.Base), 
    #               sus.optional (dict), 
    #               sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    # @sus.returns (sus.nothing)
    def __init__ (self, id=None, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 

        print id
        print session
        print _adaptor
        print _adaptor_state
        print _ttype
        
        self._resrc = super  (Compute, self)
        self._resrc.__init__ (id, session, _adaptor, _adaptor_state, _ttype)

        if  self.rtype != const.COMPUTE :
            raise se.BadParameter ("Cannot initialize Compute resource with %s id" \
                                % self.rtype)


# ------------------------------------------------------------------------------
#
class Storage (Resource) :
    """
    A Storage resource is a resource which can store data.  As such,
    the 'Access' attribute of the storage resource (a URL) can be used to create
    a :class:`saga.filesystem.Directory` instance to manage the resource's data
    space.
    """

    # --------------------------------------------------------------------------
    #
    @sus.takes   ('StorageResource', 
                  sus.optional (basestring), 
                  sus.optional (ss.Session),
                  sus.optional (sab.Base), 
                  sus.optional (dict), 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (sus.nothing)
    def __init__ (self, id=None, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        
        self._resrc = super  (Storage, self)
        self._resrc.__init__ (id, session, _adaptor, _adaptor_state, _ttype)

        if  self.rtype != const.STORAGE :
            raise se.BadParameter ("Cannot initialize Storage resource with %s id" \
                                % self.rtype)


# ------------------------------------------------------------------------------
#
class Network (Resource) :
    """
    A Network resource is a resource which can connect arbitrary resources.
    """

    # --------------------------------------------------------------------------
    #
    @sus.takes   ('NetworkResource', 
                  sus.optional (basestring), 
                  sus.optional (ss.Session),
                  sus.optional (sab.Base), 
                  sus.optional (dict), 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (sus.nothing)
    def __init__ (self, id=None, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        
        self._resrc = super  (Network, self)
        self._resrc.__init__ (id, session, _adaptor, _adaptor_state, _ttype)

        if  self.rtype != const.NETWORK :
            raise se.BadParameter ("Cannot initialize Network resource with %s id" \
                                % self.rtype)
# 
# ------------------------------------------------------------------------------


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

