# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Provides the SAGA runtime. """

import signal
import string
import sys

from   saga.utils.singleton import Singleton
from   saga.engine.config   import Configurable, getConfig
from   saga.engine.logger   import Logger,       getLogger

import saga.engine.registry 
import saga.task
import saga.utils.exception


##################################################################################
# a define to make get_adaptor more readable
ANY_ADAPTOR = None

############# These are all supported options for saga.engine ####################
##
_config_options = [
    { 
    'category'      : 'saga.engine',
    'name'          : 'foo', 
    'type'          : str, 
    'default'       : 'bar', 
    'valid_options' : None,
    'documentation' : 'dummy config option for unit test.',
    'env_variable'  : None
    },
    { 
    'category'      : 'saga.engine',
    'name'          : 'enable_ctrl_c', 
    'type'          : bool, 
    'default'       : True,
    'valid_options' : [True, False],
    'documentation' : 'install SIGINT signal handler to abort application.',
    'env_variable'  : None
    },
    { 
    'category'      : 'saga.engine',
    'name'          : 'load_beta_adaptors', 
    'type'          : bool, 
    'default'       : False,
    'valid_options' : [True, False],
    'documentation' : 'load adaptors which are marked as beta (i.e. not released).',
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

          _cpis = 
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
              
              for adaptor_class in self._engine._cpis{'job'}{'http'}
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
        
        # Engine manages cpis from adaptors
        self._cpis     = {}

        # set the configuration options for this object
        Configurable.__init__(self, 'saga.engine', _config_options)
        self._cfg = self.get_config()


        # Initialize the logging
        Logger()
        self._logger = getLogger ('saga.engine')


        # install signal handler, if requested
        if self._cfg['enable_ctrl_c'].get_value () :

            self._logger.debug ("installing signal handler for SIGKILL")

            def signal_handler(signal, frame):
                sys.stderr.write("Ctrl+C caught. Exiting...")
                sys.exit(0)

            signal.signal(signal.SIGINT, signal_handler)


        # load adaptors
        self._load_adaptors()




    #-----------------------------------------------------------------
    # 
    def _load_adaptors(self, inject_registry=False):
        """ Try to load all adaptors that are registered in 
            saga.engine.registry.py. This method is called from the constructor. 

            :param inject_registry: Inject a fake registry. *For unit tests only*.
        """
        global_config = getConfig()

        # check if we support alpha/beta adaptos
        allow_betas = self._cfg['load_beta_adaptors'].get_value ()


        if inject_registry is False:
            registry = saga.engine.registry.adaptor_registry
        else:
            self._cpis = {} # reset cpi infos
            registry   = inject_registry

        for module_name in registry:

            self._logger.info("Found entry for adaptor module '%s' in registry"  %  module_name)

            try :
                adaptor_module = __import__ (module_name, fromlist=['Adaptor'])

                # we expect the module to have an 'register' method implemented,
                # which returns a info dict for all implemented CPI classes
                adaptor_info = None
                try: 
                    adaptor_instance = adaptor_module.Adaptor ()
                    adaptor_info     = adaptor_instance.register ()
                except Exception, ex:
                    self._logger.warning("Loading %s failed: %s" % (module_name, str(ex)))
                    self._logger.debug(saga.utils.exception.get_traceback ())
                    continue # skip to next adaptor

                # No exception, but adaptor_info is empty
                if adaptor_info is None :
                    self._logger.warning("Loading %s failed: register() returned no usable adaptor info" % module_name)
                    self._logger.debug(saga.util.exception._get_traceback ())
                    continue # skip to next adaptor

                adaptor_name    = adaptor_info['name']
                adaptor_version = adaptor_info['version']

                adaptor_enabled = True 

                # default to 'disabled' if adaptor ids alpha or beta, and config
                # does disable those
                if 'alpha' in adaptor_version.lower() or \
                   'beta'  in adaptor_version.lower()    :

                    if not allow_betas :
                        self._logger.info ("Not loading adaptor %s[%s] from module %s: beta versions are disabled" \
                                        % (adaptor_name, adaptor_version, module_name))
                        continue

                # try to find an 'enabled' option in the adaptor's config
                # section, default to True
                try :
                    adaptor_config  = global_config.get_category (adaptor_name)
                    adaptor_enabled = adaptor_config['enabled'].get_value ()

                except Exception as e :
                    # ignore non-options...
                    pass

                # only load adaptor if it is not disabled via config files
                if adaptor_enabled in ["False", False] :
                    self._logger.info("Not loading %s from module %s: 'enabled' set to False" \
                      % (adaptor_name, module_name))
                    continue
                else :
                    pass
                    self._logger.info("Successfully loaded %s[%s] from module %s" \
                      % (adaptor_name, adaptor_version, module_name))


                # we got an adaptor info struct
                if not 'cpis' in adaptor_info :
                    self._logger.warn("adaptor %s does not register any cpis")
                    continue

                for cpi_info in adaptor_info['cpis'] :

                    # dig cpi information from registry. Missing keys are
                    # rightly expected to raise an exception
                    cpi_type      = cpi_info['type']
                    cpi_classname = cpi_info['class']
                    cpi_schemas   = cpi_info['schemas']
                    cpi_enabled   = True  # default
                    cpi_fullname  = "%s.%s.%s"  %  (module_name, adaptor_name, cpi_classname)

                    # register adaptor class for the listed URL schemas (once)
                    for cpi_schema in cpi_schemas :

                        cpi_schema = cpi_schema.lower ()

                        cpi_class  = getattr (adaptor_module, cpi_classname)

                        if not cpi_type in self._cpis :
                            self._cpis[cpi_type] = {}

                        if not cpi_schema in self._cpis[cpi_type] :
                            self._cpis[cpi_type][cpi_schema] = []

                        info = {'cpi_class'        : cpi_class, 
                                'adaptor_instance' : adaptor_instance}
                        if not info in self._cpis[cpi_type][cpi_schema] :
                            self._cpis[cpi_type][cpi_schema].append (info)


            except Exception as e:
                self._logger.warn("Loading %s failed: %s" % (module_name, str(e)))
                self._logger.debug(saga.utils.exception.get_traceback())

        # self._dump()


    #-----------------------------------------------------------------
    # 
    def find_adaptors (self, ctype, schema) :
        '''
        Look for a suitable cpi class serving a particular schema
        '''

        adaptor_names = []

        schema = schema.lower ()

        if not ctype in self._cpis :
            return []

        if not schema in self._cpis[ctype] :
            return []


        for info in self._cpis[ctype][schema] :

            adaptor_instance = info['adaptor_instance']
            adaptor_name     = adaptor_instance.get_name ()
            adaptor_names.append (adaptor_name)

        return adaptor_names



    #-----------------------------------------------------------------
    # 
    def get_adaptor (self, api_instance, ctype, schema, ttype, requested_name, *args, **kwargs) :
        '''
        Look for a suitable cpi class for bind, and instantiate it.
        
        If 'requested_name' is given, only matching adaptors are considered, and
        the resulting adaptor classes are not initialized.  This code path is
        used to re-bind to existing adaptors.
        '''
        schema = schema.lower ()

        #self._logger.debug(": '%s - %s - %s' "  %  (ctype, schema, requested_name))

        if not ctype in self._cpis :
            raise saga.exceptions.NotImplemented ("No adaptor class found for '%s' and URL scheme %s://" % (ctype, schema))

        if not schema in self._cpis[ctype] :
            raise saga.exceptions.NotImplemented ("No adaptor class found for '%s' and URL scheme %s://" %  (ctype, schema))


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
        for info in self._cpis[ctype][schema] :

            cpi_class        = info['cpi_class']
            adaptor_instance = info['adaptor_instance']
            adaptor_name     = adaptor_instance.get_name ()

            try :
                # instantiate cpi
                cpi_instance = cpi_class (api_instance, adaptor_instance)
                cpi_name     = cpi_instance.get_cpi_name ()

                if requested_name != None :
                    if requested_name == adaptor_name :
                        return cpi_instance

                    # ignore this adaptor
                    self._logger.debug ("get_adaptor %s.%s -- ignore %s != %s" \
                                          %  (adaptor_name, cpi_name, requested_name, adaptor_name))
                    continue


                if ttype == None :
                    # run the sync constructor for sync construction, and return
                    # the adaptor_instance to bind to the API instance.
                    cpi_instance.init_instance  (*args, **kwargs)

                    self._logger.debug("BOUND get_adaptor %s.%s -- success"
                            %  (adaptor_name, cpi_name))
                    return cpi_instance

                else :
                    # the async constructor will return a task, which we pass
                    # back to the caller (instead of the adaptor instance). That 
                    # task is responsible for binding the adaptor to the later 
                    # returned API instance.
                    self._logger.debug("get_adaptor %s.%s -- async task creation"  %  (adaptor_name, cpi_name))

                    task = cpi_instance.init_instance_async (ttype, *args, **kwargs)
                    return task


            except Exception as e :
                # adaptor class initialization failed - try next one
                m    = "%s.%s: %s"  %  (adaptor_name, cpi_class, str(e))
                msg += "\n  %s" % m
                self._logger.info("get_adaptor %s", m)
                continue

        self._logger.error ("No suitable adaptor found for '%s' and URL scheme '%s'" %  (ctype, schema))
        raise saga.exceptions.NotImplemented ("No suitable adaptor found: %s" %  msg)


    #-----------------------------------------------------------------
    # 
    def loaded_cpis (self):
        return self._cpis


    #-----------------------------------------------------------------
    # 
    def _dump (self) :
        import pprint
        pprint.pprint (self._cpis)

