# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Provides the SAGA runtime. """


from   saga.utils.singleton import Singleton
from   saga.engine.config   import Configurable, getConfig
from   saga.engine.logger   import Logger,       getLogger

import saga.engine.registry 
import saga.task
import saga.exceptions


##################################################################################
# a define to make get_adaptor more readable
ANY_ADAPTOR = None

############# These are all supported options for saga.engine ####################
##
_all_engine_config_options = [
    { 
    'category'      : 'saga.engine',
    'name'          : 'foo', 
    'type'          : str, 
    'default'       : 'bar', 
    'valid_options' : None,
    'documentation' : 'dummy config option for unit test.',
    'env_variable'  : None
    }
]

################################################################################
##
def getEngine():
    """ Return a handle to the Engine object.
    """
    return Engine() 


################################################################################
##
class Engine(Configurable): 
    """ Represents the SAGA engine runtime system.

        The Engine class is a singleton class that takes care of 
        configuration, logging and adaptor management. Engine is 
        instantiated implicitly as soon as SAGA is imported into
        Python. It can be used to introspect the current state of
        a SAGA instance.

        The Engine singleton will, on creation, load all available 
        adaptors.  Adaptors modules MUST implement a register() 
        function which returns a list of dicts like this::

          [
            { 
              'name'    : _adaptor_name,
              'type'    : 'saga.job.Job',
              'class'   : 'LocalJob',
              'schemas' : ['fork', 'local']
            }, 
            { 
              'name'    : _adaptor_name,
              'type'    : 'saga.job.Service',
              'class'   : 'LocalJobService',
              'schemas' : ['fork', 'local']
            } 
          ]

        where 'class' points to the actual adaptor classes, 
        and 'schemas' lists the URL schemas for which those 
        adaptor classes should be considered.  Note that 
        schemas are case insensitive.

        While loading adaptors, the Engine builds up a registry 
        of adaptor classes, hierarchically sorted like this::

          _adaptors = 
          { 
              'job' : 
              { 
                  'gram' : [<gram job  adaptor class>]
                  'ssh'  : [<ssh  job  adaptor class>]
                  'http' : [<aws  job  adaptor class>,
                            <occi job  adaptor class>]
                  ...
              },
              'file' : 
              { 
                  'ftp'  : <ftp  file adaptor class>
                  'scp'  : <scp  file adaptor class>
                  ...
              },
              ...
          }

        to enable simple lookup operations when binding an API object to an
        adaptor class instance.  For example, a 
        'saga.job.Service('http://remote.host.net/')' constructor would use
        (simplified)::

          def __init__ (self, url, session=None) :
              
              for adaptor_class in self._engine._adaptors{'job'}{'http'}
                  try :
                      self._adaptor = adaptor_class (self, url, session}
                  except saga.Exception e :
                      # adaptor could not handle the URL, handle e
                  else :
                      # successfully bound to adaptor
                      return

        Adaptors to be loaded are searched for, by default in the module's
        'adaptors/' subdirectory.  The config option 'adaptor_path' can 
        specify additional (comma separated) paths to search.  The engine 
        will attempt to load any python module named 'saga_adaptor_[name].py'
        in any of the specified paths (default path first, then configured 
        paths in the specified order).
        '
    """
    __metaclass__ = Singleton


    #-----------------------------------------------------------------
    # 
    def __init__(self):
        
        # Engine manages adaptors
        self._adaptors = {}

        # set the configuration options for this object
        Configurable.__init__(self, 'saga.engine', _all_engine_config_options)

        # initialize logging
        self._initialize_logging()

        # load adaptors
        self._load_adaptors()


    #-----------------------------------------------------------------
    # 
    def _initialize_logging(self):
        """ Initialize the logging facilites. 
        """
        Logger()
        self._logger = getLogger('saga.engine')


    #-----------------------------------------------------------------
    # 
    def _load_adaptors(self, inject_registry=False):
        """ Try to load all adaptors that are registered in 
            saga.engine.registry.py. This method is called from the constructor. 

            :param inject_registry: Inject a fake registry. *For unit tests only*.
        """
        global_config = getConfig()

        if inject_registry is False:
            registry = saga.engine.registry.adaptor_registry
        else:
            self._adaptors = {} # reset adaptor list
            registry = inject_registry

        for module_name in registry:

            self._logger.info("Found entry for adaptor module '%s' in registry"  %  module_name)

            try :
                adaptor_module = __import__ (module_name, fromlist=['register'])

                # we expect the module to have an 'register' method implemented,
                # which returns a info dict for all implemented CPI classes
                adaptor_infos = None
                try: 
                    adaptor_infos = adaptor_module.register ()
                except Exception, ex:
                    self._logger.warning("Loading %s failed: %s" % (module_name, str(ex)))
                    self._logger.debug(saga.exceptions._get_traceback ())
                    continue # skip to next adaptor

                # No exception, but adaptor_infos is empty
                if adaptor_infos is None :
                    self._logger.warning("Loading %s failed: register() returned no usable adaptor info" % module_name)
                    self._logger.debug(saga.exceptions._get_traceback ())
                    continue # skip to next adaptor

                # we got an adaptor info struct
                for adaptor_info in adaptor_infos :

                    # dig adaptor information from registry. Missing keys are
                    # rightly expected to raise an exception
                    adaptor_name     = adaptor_info['name']
                    adaptor_type     = adaptor_info['type']
                    adaptor_class    = adaptor_info['class']
                    adaptor_schemas  = adaptor_info['schemas']
                    adaptor_enabled  = True  # default
                    adaptor_fullname = "%s.%s"  %  (module_name, adaptor_class)

                    # try to find an 'enabled' option in the adaptor's config section
                    adaptor_opts = list()
                    try :
                        adaptor_config  = global_config.get_category (module_name)
                        for (k,v) in adaptor_config.iteritems():
                            adaptor_opts.append(v.as_dict())

                        adaptor_enabled = adaptor_config['enabled'].get_value ()

                    except Exception as e :
                        pass
                    self._logger.info('Config options for %s: %s' % (module_name, adaptor_opts))

                    # only load adaptor if it is not disabled via config files
                    if adaptor_enabled in ["False", False] :
                        self._logger.info("Not loading %s from module %s: 'enabled' set to False" \
                          % (adaptor_class, module_name))
                        continue
                    else :
                        pass
                        self._logger.info("Successfully loaded %s from module %s" \
                          % (adaptor_class, module_name))

                    # register adaptor class for the listed URL schemas (once)
                    for adaptor_schema in adaptor_schemas :

                        adaptor_schema = adaptor_schema.lower ()

                        adp_class = getattr (adaptor_module, adaptor_class)

                        if not adaptor_type in self._adaptors :
                            self._adaptors[adaptor_type] = {}

                        if not adaptor_schema in self._adaptors[adaptor_type] :
                            self._adaptors[adaptor_type][adaptor_schema] = []

                        if not adp_class in self._adaptors[adaptor_type][adaptor_schema] :
                            self._adaptors[adaptor_type][adaptor_schema].append (adp_class)
                        

            except Exception as e:
                self._logger.warn("Loading %s failed: %s" % (module_name, str(e)))
                self._logger.debug(saga.exceptions._get_traceback ())


    #-----------------------------------------------------------------
    # 
    def get_adaptor (self, api, ctype, schema, ttype, requested_name, *args, **kwargs) :
        '''
        just as get_adaptor, look for a suitable adaptor for bind -- but only
        consider adaptors with matching name.  This method is used to force
        binding to a specific adaptor, for example when a job.Service adaptor
        creates a job.Job instance, and wants it bound to the same adaptor.
        '''

        schema = schema.lower ()

        self._logger.debug("get_adaptor: '%s - %s - %s' "  %  (ctype, schema, requested_name))

        if not ctype in self._adaptors :
            raise saga.exceptions.NotImplemented ("no adaptor found for '%s'" %  ctype)

        if not schema in self._adaptors[ctype] :
            raise saga.exceptions.NotImplemented ("no '%s' adaptor found for '%s'" %  (ctype, schema))


        # cycle through all applicable adaptors, and try to instantiate the ones
        # with matching name.  
        # If that works, and ttype signals a sync object construction, call the 
        # init_instance(), which effectively performs the semantics of the API
        # level object constructor.  For asynchronous object instantiation (via
        # create factory methods), the init_instance_async will be called from
        # API level -- but at that point will not be able to abort the adaptor
        # binding if the constructor semantics signals a problem (i.e. cannot
        # handle URL after all).
        msg = ""
        for adaptor_class in self._adaptors[ctype][schema] :

            adaptor_name = ""
            try :
                # instantiate adaptor
                adaptor_instance = adaptor_class (api)
                adaptor_name     = adaptor_instance._get_name ()

                if requested_name != None     and \
                   requested_name != adaptor_name :
                    # ignore this adaptor
                    self._logger.debug("get_adaptor %s -- ignore %s != %s" \
                                    % (str(adaptor_class), requested_name, adaptor_name))
                    return adaptor_instance
                    continue

                if ttype == None or ttype == saga.task.SYNC :
                    # run the sync constructor for sync construction, and return
                    # the adaptor_instance to bind to the API instance.
                    adaptor_instance.init_instance  (*args, **kwargs)

                    self._logger.debug("get_adaptor %s -- success"  %  str(adaptor_class))
                    return adaptor_instance

                else :
                    # the async constructor will return a task, which we pass
                    # back to the caller (instead of the adaptor instance). That 
                    # task is responsible for binding the adaptor to the later 
                    # returned API instance.
                    self._logger.debug("get_adaptor %s -- async task creation"  %  str(adaptor_class))

                    task = adaptor_instance.init_instance_async (ttype, *args, **kwargs)
                    return task


            except Exception as e :
                # adaptor class initialization failed - try next one
                self._logger.info("get_adaptor %s -- failed: %s"  \
                               % (str(adaptor_class), str(e)))
                msg += "\n  %s: %s"  %  (adaptor_name, str(e))
                continue

        raise saga.exceptions.NotImplemented ("no suitable adaptor found: %s" %  msg)


    #-----------------------------------------------------------------
    # 
    def loaded_adaptors(self):
        return self._adaptors

