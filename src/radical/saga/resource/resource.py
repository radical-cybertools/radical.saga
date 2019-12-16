
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import radical.utils               as ru
import radical.utils.signatures    as rus

from ..constants import SYNC, ASYNC, TASK
from ..adaptors  import base       as sab

from .. import sasync
from .. import task                as st
from .. import base                as sb
from .. import session             as ss
from .. import exceptions          as se
from .. import attributes          as sa
from .. import constants           as sc

from .  import description         as descr
from .  import constants           as c


# ------------------------------------------------------------------------------
#
class Resource (sb.Base, sa.Attributes, sasync.Async) :
    """
    A :class:`Resource` class instance represents a specific slice of resource
    which is, if in `RUNNING` state, under the applications control and ready to
    serve usage requests.  The type of accepted usage requests depends on the
    specific resource types (job execution for :class:`saga.resource.Compute`,
    data storage for :class:`saga.resource.Storage`, and network connectivity
    for :class:`saga.resource.Network`.  The exact mechanism how those usage
    requests are communicated are not part of the resource's class interface,
    but are instead served by other RADICAL-SAGA classes -- typically those are
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
    different ways: they can be actively be destroyed by the application, and
    will then enter the `CANCELED` state; they can internally cease to function
    and become unable to serve usage requests, represented by a `FAILED` state,
    and the resource manager can retract control from the application because
    the agreed time duration has passed -- this is represented by the `EXPIRED`
    state.
    """
    # FIXME:
    #   - we don't use PENDING like this, yet
    #   - include state diagram (also for jobs btw)

    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Resource',
                  rus.optional (str),
                  rus.optional (ss.Session),
                  rus.optional (sab.Base),
                  rus.optional (dict),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (rus.nothing)
    def __init__ (self, id=None, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) :
        """
        __init__(id=None, session=None)

        Create / reconnect to a resource.

        :param id: id of the resource
        :type  id: :class:`saga.Url`

        :param session: :class:`saga.Session`

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

        import radical.saga.attributes as sa

        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface

        self._attributes_register(c.ID          , None, sa.ENUM,   sa.SCALAR, sa.READONLY)
        self._attributes_register(c.RTYPE       , None, sa.ENUM,   sa.SCALAR, sa.READONLY)
        self._attributes_register(c.STATE       , None, sa.ENUM,   sa.SCALAR, sa.READONLY)
        self._attributes_register(c.STATE_DETAIL, None, sa.STRING, sa.SCALAR, sa.READONLY)
        self._attributes_register(c.ACCESS      , None, sa.URL,    sa.SCALAR, sa.READONLY)
        self._attributes_register(c.MANAGER     , None, sa.URL,    sa.SCALAR, sa.READONLY)
        self._attributes_register(c.DESCRIPTION , None, sa.ANY,    sa.SCALAR, sa.READONLY)

        self._attributes_set_enums (c.STATE, [c.UNKNOWN ,
                                              c.PENDING ,
                                              c.ACTIVE  ,
                                              c.CANCELED,
                                              c.EXPIRED ,
                                              c.FAILED  ,
                                              c.FINAL   ])

        self._attributes_set_enums (c.RTYPE, [c.COMPUTE ,
                                              c.STORAGE ,
                                              c.NETWORK ])


        self._attributes_set_getter (c.ID          , self.get_id          )
        self._attributes_set_getter (c.RTYPE       , self.get_rtype       )
        self._attributes_set_getter (c.STATE       , self.get_state       )
        self._attributes_set_getter (c.STATE_DETAIL, self.get_state_detail)
        self._attributes_set_getter (c.ACCESS      , self.get_access      )
        self._attributes_set_getter (c.MANAGER     , self.get_manager     )
        self._attributes_set_getter (c.DESCRIPTION , self.get_description )


        # FIXME: we need the ID to be or to include an URL, as we don't have
        # a scheme otherwise, which means we can't select an adaptor.  Duh! :-/

        # FIXME: documentation for attributes is missing.

        # param checks
        scheme = None
        if  not id :
            if  'resource_schema' not in _adaptor_state :
                raise se.BadParameter ("Cannot initialize resource without id"
                                    % self.rtype)
            else :
                scheme = _adaptor_state['resource_schema']
        else :
            # ID is formatted as '[manager-url]-[resource-id]'
            import parse   # FIXME: use regex to reduce number of dependencies
            res      = parse.parse('[{}]-[{}]', id)
            url      = ru.Url(res[0])
            scheme   = url.scheme.lower ()

        if not session :
            session = ss.Session (default=True)

        self._base  = super (Resource, self)
        self._base.__init__ (scheme, _adaptor, _adaptor_state, id, session,
                             ttype=_ttype)


    # --------------------------------------------------------------------------
    #
    @classmethod
    @rus.takes   ('resource',
                  rus.optional (str),
                  rus.optional (ss.Session),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (st.Task)
    def create   (cls, id=None, session=None, ttype=sc.SYNC) :
        """
        This is the asynchronous class constructor, returning
        a :class:`saga:Task` instance.  For details on the accepted parameters,
        please see the description of :func:`__init__`.
        """

        return cls (id, session, _ttype=ttype)._init_task


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Resource',
                  descr.Description,
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
    def reconfig (self, descr, ttype=None) :
        """
        reconfig(descr)

        A resource is acquired according to a resource description, i.e. to
        a specific set of attributes.  At some point in time, while the
        resource is running, the application requirements on the resource may
        have changed -- in that case, the application can request to change the
        resource's configuration on the fly.

        This method cannot be used to change the type of the resource.  Backends
        may or may not support this operation -- if not,
        a :class:`saga.NotImplemented` exception is raised.  If the method is
        supported, , then the semantics of the method is equivalent to the
        semantics of the :func:`acquire` call on the
        :class:`saga.resource.Manager` class.
        """

        return self._adaptor.reconfig (descr, ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Resource',
                  str,
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
    def destroy  (self, ttype=None) :
        """
        destroy()

        The semantics of this method is equivalent to the semantics of the
        :func:`destroy` call on the :class:`saga.resource.Manager` class.
        """

        return self._adaptor.destroy (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Resource',
                  rus.optional (rus.one_of (c.UNKNOWN, c.NEW,      c.PENDING,
                                            c.ACTIVE,  c.DONE,     c.FAILED,
                                            c.EXPIRED, c.CANCELED, c.FINAL)),
                  rus.optional (float),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
    def wait (self, state=c.FINAL, timeout=None, ttype=None) :
        """
        wait(state=FINAL, timeout=None)

        Wait for a resource to enter a specific state.

        :param state: resource state to wait for (UNKNOWN, NEW, PENDING, ACTIVE,
                      DONE, FAILED, EXPIRED, CANCELED, FINAL)

        :type  state: float
        :param state: time to block while waiting.

        This method will block until the resource entered the specified state,
        or until `timeout` seconds have passed -- whichever occurs earlier.  If
        the resource is in a final state, the call will raise and
        :class:`saga.IncorrectState` exception when asked to wait for any
        non-final state.

        A negative `timeout` value represents an indefinit timeout.
        """

        # FIXME:
        #   - right now, we can not put a resource in a `TaskContainer`, because
        #     it is not a `Task`.  We need to either reconsider the class
        #     hierarchy, and then inherit a `ResourceContainer` from
        #     `TaskContainer` (see job); or we implement a `ResourceContainer`
        #     from scratch...

        return self._adaptor.wait (state, timeout, ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Resource', rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, str, st.Task))
    def get_id   (self, ttype=None) :
        """
        get_id()

        Return the resource ID.
        """
        return self._adaptor.get_id            (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @rus.takes    ('Resource', rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns  ((rus.one_of (c.COMPUTE,
                                c.STORAGE,
                                c.NETWORK), st.Task))
    def get_rtype (self, ttype=None) :
        """
        get_rtype()

        Return the resource type.
        """
        return self._adaptor.get_rtype         (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @rus.takes    ('Resource', rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns  ((rus.one_of (c.UNKNOWN ,
                                c.NEW     ,
                                c.PENDING ,
                                c.ACTIVE  ,
                                c.CANCELED,
                                c.EXPIRED ,
                                c.DONE    ,
                                c.FAILED  ,
                                c.FINAL   ), st.Task))
    def get_state (self, ttype=None) :
        """
        get_state()

        Return the state of the resource.
        """
        return self._adaptor.get_state         (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Resource', rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, str, st.Task))
    def get_state_detail (self, ttype=None) :
        """
        get_state_detail()

        Return the state details (backend specific) of the resource.
        """
        return self._adaptor.get_state_detail  (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @rus.takes     ('Resource', rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns   ((rus.nothing, str, st.Task))
    def get_access (self, ttype=None) :
        """
        get_access()

        Return the resource access Url.
        """
        return self._adaptor.get_access        (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @rus.takes      ('Resource', rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns    ((str, st.Task))
    def get_manager (self, ttype=None) :
        """
        get_manager()

        Return the manager instance that was used to acquire this resource.
        """
        return self._adaptor.get_manager       (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Resource', rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, descr.Description, st.Task))
    def get_description  (self, ttype=None) :
        """
        get_description()

        Return the description that was used to aquire this resource.
        """
        return self._adaptor.get_description   (ttype=ttype)


# ------------------------------------------------------------------------------
#
class Compute (Resource) :
    """
    A Compute resource is a resource which provides compute capabilities, i.e.
    which can execute compute jobs.  As such, the 'Access' attribute of the
    compute resource (a URL) can be used to create a :class:`saga.job.Service`
    instance to submit jobs to.
    """

    # --------------------------------------------------------------------------
    # FIXME: should 'ACCESS' be a list of URLs?  A VM could have an ssh *and*
    #        a gram endpoint...

    # @rus.takes   ('ComputeResource',
    #               rus.optional (basestring),
    #               rus.optional (ss.Session),
    #               rus.optional (sab.Base),
    #               rus.optional (dict),
    #               rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    # @rus.returns (rus.nothing)
    def __init__ (self, id=None, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) :

        self._resrc = super  (Compute, self)
        self._resrc.__init__ (id, session, _adaptor, _adaptor_state, _ttype)

        if  self.rtype != c.COMPUTE :
            raise se.BadParameter ("Cannot init Compute resource type %s"
                                % self.rtype)


# ------------------------------------------------------------------------------
#
class Storage (Resource) :
    """
    A Storage resource is a resource which has storage capabilities, i.e. the
    ability to persistently store, organize and retrieve data.  As such, the
    'Access' attribute of the storage resource (a URL) can be used to create
    a :class:`saga.filesystem.Directory` instance to manage the resource's data
    space.
    """

    # --------------------------------------------------------------------------
    #
    @rus.takes   ('StorageResource',
                  rus.optional (str),
                  rus.optional (ss.Session),
                  rus.optional (sab.Base),
                  rus.optional (dict),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (rus.nothing)
    def __init__ (self, id=None, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) :

        self._resrc = super  (Storage, self)
        self._resrc.__init__ (id, session, _adaptor, _adaptor_state, _ttype)

        if  self.rtype != c.STORAGE :
            raise se.BadParameter ("Cannot init Storage resource type %s"
                                % self.rtype)


# ------------------------------------------------------------------------------
#
class Network (Resource) :
    """
    A Network resource is a resource which has network capabilities.
    """

    # --------------------------------------------------------------------------
    #
    @rus.takes   ('NetworkResource',
                  rus.optional (str),
                  rus.optional (ss.Session),
                  rus.optional (sab.Base),
                  rus.optional (dict),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (rus.nothing)
    def __init__ (self, id=None, session=None,
                  _adaptor=None, _adaptor_state={}, _ttype=None) :

        self._resrc = super  (Network, self)
        self._resrc.__init__ (id, session, _adaptor, _adaptor_state, _ttype)

        if  self.rtype != c.NETWORK :
            raise se.BadParameter ("Cannot init Network resource type %s"
                                % self.rtype)
#
# ------------------------------------------------------------------------------




