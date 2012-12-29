# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Provides the SAGA runtime. """

import re
import sys
import pprint
import string
import signal
import inspect

from   saga.utils.singleton import Singleton
from   saga.engine.logger   import getLogger, get_traceback
from   saga.engine.config   import getConfig, Configurable

import saga.engine.registry  # adaptors to load
import saga.cpi              # load cpi's so that we can check what adaptors implement


##################################################################################
# a define to make bind_adaptor calls more readable
ANY_ADAPTOR = None

############# These are all supported options for saga.engine ####################
##
_config_options = [
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

    """ Return a handle to the Engine singleton."""
    return Engine() 


################################################################################
##
class Engine(Configurable): 
    """ Represents the SAGA engine runtime system.

        The Engine is a singleton class that takes care of adaptor
        loading and management, and which binds adaptor instances to
        API object instances.   The Engine singleton is implicitly
        instantiated as soon as SAGA is imported into Python.  It
        will, on creation, load all available adaptors.  Adaptors
        modules MUST provide an 'Adaptor' class, which will register
        the adaptor in the engine with information like these
        (simplified)::

          _ADAPTOR_INFO = {
            'name'    : _adaptor_name,
            'version' : 'v1.3'
            'schemas' : ['fork', 'local']
            'cpis'    : [{ 
              'type'    : 'saga.job.Job',
              'class'   : 'LocalJob',
              }, 
              { 
              'type'    : 'saga.job.Service',
              'class'   : 'LocalJobService',
              } 
            ]
          }

        where 'class' points to the actual adaptor classes, and
        'schemas' lists the URL schemas for which those adaptor
        classes should be considered.  Note that schemas are case
        insensitive.  More details on the adaptor registration process
        and on adaptor meta data can be found in the adaptors writer
        guide.

        :todo: add link to adaptor writers documentation.

        While loading adaptors, the Engine builds up an internal
        registry of adaptor classes, hierarchically sorted like this
        (simplified)::

          _adaptor_registry = 
          { 
              'job' : 
              { 
                  'gram' : [<gram job  adaptor, gram job adaptor class>]
                  'ssh'  : [<ssh  job  adaptor, ssh  job adaptor class>]
                  'http' : [<aws  job  adaptor, aws  job adaptor class>,
                            <occi job  adaptor, occi job adaptor class>]
                  ...
              },
              'file' : 
              { 
                  'ftp'  : <ftp file adaptor, ftp file adaptor class>
                  'scp'  : <scp file adaptor, scp file adaptor class>
                  ...
              },
              ...
          }

        to enable simple lookup operations when binding an API object
        to an adaptor class instance.  For example, a
        'saga.job.Service('http://remote.host.net/')' constructor
        would use (simplified)::

          def __init__ (self, url="", session=None) :
              
              for (adaptor, adaptor_class) in self._engine._adaptor_registry{'job'}{url.scheme}

                  try :
                      self._adaptor = adaptor_class (self, url, session}

                  except saga.Exception e :
                      # adaptor bailed out
                      continue

                  else :
                      # successfully bound to adaptor
                      return

    """
    __metaclass__ = Singleton


    #-----------------------------------------------------------------
    # 
    def __init__(self):
        
        # Engine manages cpis from adaptors
        self._adaptor_registry = {}


        # set the configuration options for this object
        Configurable.__init__(self, 'saga.engine', _config_options)
        self._cfg = self.get_config()


        # Initialize the logging
        self._logger = getLogger ('saga.engine')


        # install signal handler, if requested
        if self._cfg['enable_ctrl_c'].get_value () :

            def signal_handler (signal, frame):
                sys.stderr.write ("Ctrl+C caught. Exiting...")
                sys.exit (0)

            self._logger.debug ("installing signal handler for SIGKILL")
            signal.signal (signal.SIGINT, signal_handler)


        # load adaptors
        self._load_adaptors ()




    #-----------------------------------------------------------------
    # 
    def _load_adaptors (self, inject_registry=None):
        """ Try to load all adaptors that are registered in 
            saga.engine.registry.py. This method is called from the
            constructor.  As Engine is a singleton, this method is
            called once after the module is first loaded in any python
            application.

            :param inject_registry: Inject a fake registry. *For unit tests only*.
        """

        # get the engine config options
        global_config = getConfig()


        # get the list of adaptors to load
        registry = saga.engine.registry.adaptor_registry


        # check if some unit test wants to use a special registry.  If
        # so, we reset cpi infos from the earlier singleton creation.
        if inject_registry != None :
            self._adaptor_registry = {}
            registry               = inject_registry


        # attempt to load all registered modules
        for module_name in registry:

            self._logger.info ("Loading  adaptor %s"  %  module_name)


            # first, import the module
            adaptor_module = None
            try :
                adaptor_module = __import__ (module_name, fromlist=['Adaptor'])

            except Exception as e:
                self._logger.error ("Skipping adaptor %s: module loading failed: %s" \
                                %  (module_name, str(e)))
                self._logger.debug (get_traceback())
                continue # skip to next adaptor


            # we expect the module to have an 'Adaptor' class
            # implemented, which, on calling 'register()', returns
            # a info dict for all implemented adaptor classes.
            adaptor_instance = None
            adaptor_info     = None
            try: 
                adaptor_instance = adaptor_module.Adaptor ()
                adaptor_info     = adaptor_instance.register ()

            except Exception as e:
                self._logger.error ("Skipping adaptor %s: loading failed: %s" \
                                 % (module_name, str(e)))
                self._logger.debug (get_traceback ())
                continue # skip to next adaptor


            # the adaptor must also provide a sanity_check() method, which sould
            # be used to confirm that the adaptor can function properly in the
            # current runtime environment (e.g., that all pre-requisites and
            # system dependencies are met).
            try: 
                adaptor_instance.sanity_check ()

            except Exception as e:
                self._logger.error ("Skipping adaptor %s: failed self test: %s" \
                                 % (module_name, str(e)))
                self._logger.debug (get_traceback ())
                continue # skip to next adaptor


            # check if we have a valid adaptor_info
            if adaptor_info is None :
                self._logger.warning ("Skipping adaptor %s: adaptor meta data are invalid" \
                                   % module_name)
                self._logger.debug   (get_traceback ())
                continue  # skip to next adaptor


            if  not 'name'    in adaptor_info or \
                not 'cpis'    in adaptor_info or \
                not 'version' in adaptor_info or \
                not 'schemas' in adaptor_info    :
                self._logger.warning ("Skipping adaptor %s: adaptor meta data are incomplete" \
                                   % module_name)
                self._logger.debug   (get_traceback ())
                continue  # skip to next adaptor


            adaptor_name    = adaptor_info['name']
            adaptor_version = adaptor_info['version']
            adaptor_schemas = adaptor_info['schemas']
            adaptor_enabled = True   # default unless disabled by 'enabled' option or version filer

            # disable adaptors in 'alpha' or 'beta' versions -- unless
            # the 'load_beta_adaptors' config option is set to True
            if not self._cfg['load_beta_adaptors'].get_value () :

                if 'alpha' in adaptor_version.lower() or \
                   'beta'  in adaptor_version.lower()    :

                    self._logger.warn ("Skipping adaptor %s: beta versions are disabled (%s)" \
                                    % (module_name, adaptor_version))
                    continue  # skip to next adaptor


            # get the 'enabled' option in the adaptor's config
            # section (saga.cpi.base ensures that the option exists,
            # if it is initialized correctly in the adaptor class.
            adaptor_config  = None
            adaptor_enabled = False

            try :
                adaptor_config  = global_config.get_category (adaptor_name)
                adaptor_enabled = adaptor_config['enabled'].get_value ()

            except Exception as e:
                # this exception likely means that the adaptor does
                # not call the cpi.AdaptorBase initializer (correctly)
                self._logger.error ("Skipping adaptor %s: initialization failed: %s" \
                                 % (module_name, str(e)))
                self._logger.debug (get_traceback ())
                continue # skip to next adaptor


            # only load adaptor if it is not disabled via config files
            if adaptor_enabled == False :
                self._logger.info ("Skipping adaptor %s: 'enabled' set to False" \
                                % (module_name))
                continue # skip to next adaptor


            # check if the adaptor has anything to register
            if 0 == len (adaptor_info['cpis']) :
                self._logger.warn ("Skipping adaptor %s: does not register any cpis" \
                                % (module_name))
                continue # skip to next adaptor


            # we got an enabled adaptor with valid info - yay!  We can
            # now register all adaptor classes (cpi implementations).
            for cpi_info in adaptor_info['cpis'] :

                # check cpi information details for completeness
                if  not 'type'    in cpi_info or \
                    not 'class'   in cpi_info    :
                    self._logger.info ("Skipping adaptor %s cpi: cpi info detail is incomplete" \
                                    % (module_name))
                    continue # skip to next cpi info


                # adaptor classes are registered for specific API types.
                cpi_type  = cpi_info['type']
                cpi_cname = cpi_info['class']
                cpi_class = None

                try :
                    cpi_class = getattr (adaptor_module, cpi_cname)
                except Exception as e:
                    # this exception likely means that the adaptor does
                    # not call the cpi.AdaptorBase initializer (correctly)
                    self._logger.warning ("Skipping adaptor %s: adaptor class invalid %s: %s" \
                                       % (module_name, cpi_info['class'], str(e)))
                    self._logger.debug   (get_traceback ())
                    continue # skip to next adaptor

                # make sure the cpi class is a valid cpi for the given type.
                # Note that saga.job.service.Service is the same as
                # saga.job.Service -- so we also make sure the module name does
                # not have duplicated last element.  Also, the last element
                # needs to be translated from CamelCase to camel_case
                cpi_last = re.sub (r'.*\.', '',             cpi_type)
                cpi_modn = re.sub (r'^saga\.', 'saga.cpi.', cpi_type)
                cpi_modn = re.sub (r'([^.]+)\.\1$', r'\1',  cpi_modn)
                cpi_modn = re.sub (r'(.*)([a-z])([A-Z])([^\.]*)$', r'\1\2_\3\4', cpi_modn).lower ()

                # does that module exist?
                if not cpi_modn in sys.modules :
                    self._logger.error ("Skipping adaptor %s: cpi type not known: '%s'" \
                                     % (module_name, cpi_type))
                    continue # skip to next cpi info


                # so, make sure the given cpi is actually
                # implemented by the adaptor class
                cpi_ok = False
                for name, cpi_obj in inspect.getmembers (sys.modules[cpi_modn]) :
                    if name == cpi_last            and \
                       inspect.isclass (cpi_obj)   and \
                       issubclass (cpi_class, cpi_obj) :
                           cpi_ok = True

                if not cpi_ok :
                    self._logger.error ("Skipping adaptor %s: doesn't implement cpi '%s (%s)'" \
                                     % (module_name, cpi_class, cpi_type))
                    continue # skip to next cpi info


                # finally, register the cpi for all its schemas!
                for adaptor_schema in adaptor_schemas :

                    adaptor_schema = adaptor_schema.lower ()

                    # make sure we can register that cpi type
                    if not cpi_type in self._adaptor_registry :
                        self._adaptor_registry[cpi_type] = {}

                    # make sure we can register that schema
                    if not adaptor_schema in self._adaptor_registry[cpi_type] :
                        self._adaptor_registry[cpi_type][adaptor_schema] = []

                    # we register the cpi class, so that we can create
                    # instances as needed, and the adaptor instance,
                    # as that is passed to the cpi class c'tor later
                    # on (the adaptor instance is used to share state
                    # between cpi instances, amongst others)
                    info = {'cpi_cname'        : cpi_cname, 
                            'cpi_class'        : cpi_class, 
                            'adaptor_name'     : adaptor_name,
                            'adaptor_instance' : adaptor_instance}

                    # make sure this tuple was not registered, yet
                    if info in self._adaptor_registry[cpi_type][adaptor_schema] :

                        self._logger.error ("Skipping adaptor %s: already registered '%s - %s'" \
                                         % (module_name, cpi_class, adaptor_instance))
                        continue # skip to next cpi info


                    self._logger.info ("Loading  adaptor %s: '%s (%s : %s://)'" \
                                    % (module_name, cpi_class, cpi_type, adaptor_schema))
                    self._adaptor_registry[cpi_type][adaptor_schema].append (info)



    #-----------------------------------------------------------------
    # 
    def find_adaptors (self, ctype, schema) :
        ''' Look for a suitable cpi class serving a particular schema

            This method will sift through our adaptor registry (see
            '_load_adaptors()', and dig for any adaptor which marches the given
            api class type and schema.  All matching adaptors are returned (by
            name)
        '''

        if not ctype in self._adaptor_registry :
            return []

        if not schema.lower () in self._adaptor_registry[ctype] :
            return []

        adaptor_names = []
        for info in self._adaptor_registry[ctype][schema.lower ()] :
            adaptor_names.append (info['adaptor_name'])

        return adaptor_names



    #-----------------------------------------------------------------
    # 
    def get_adaptor (self, adaptor_name) :
        ''' Return the adaptor module's ``Adaptor`` class for the given adaptor
            name.  
            
            This method is used if adaptor or API object implementation need to
            interact with other adaptors.
        '''

        for ctype in self._adaptor_registry.keys () :
            for schema in self._adaptor_registry[ctype].keys () :
                for info in self._adaptor_registry[ctype][schema] :
                    if ( info['adaptor_name'] == adaptor_name ) :
                        return info['adaptor']

        raise saga.exceptions.NotSucess ("No adaptor named '%s' found"  %  adaptor_name)



    #-----------------------------------------------------------------
    # 
    def bind_adaptor (self, api_instance, ctype, schema, ttype, adaptor=None, *args, **kwargs) :
        '''
        Look for a suitable cpi class for bind, and instantiate it.
        
        If 'adaptor' is not 'None', only that given adaptors is considered, and
        adaptor classes are only created from that specific adaptor.
        '''

        schema = schema.lower ()

        if not ctype in self._adaptor_registry :
            raise saga.exceptions.NotImplemented ("No adaptor class found for '%s' and URL scheme %s://" \
                                               % (ctype, schema))

        if not schema in self._adaptor_registry[ctype] :
            raise saga.exceptions.NotImplemented ("No adaptor class found for '%s' and URL scheme %s://" \
                                               % (ctype, schema))


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
        for info in self._adaptor_registry[ctype][schema] :

            cpi_cname        = info['cpi_cname']
            cpi_class        = info['cpi_class']
            adaptor_name     = info['adaptor_name']
            adaptor_instance = info['adaptor_instance']

            try :

                # is this adaptor acceptable?
                if adaptor != None :
                    if adaptor != adaptor_instance :
                        
                        # ignore this adaptor
                        self._logger.debug ("bind_adaptor for %s : %s != %s - ignore adaptor" \
                                         % (cpi_cname, adaptor, adaptor_instance))
                        continue


                # instantiate cpi
                cpi_instance = cpi_class (api_instance, adaptor_instance)


                if ttype == None :
                    # run the sync constructor for sync construction, and return
                    # the adaptor_instance to bind to the API instance.
                    cpi_instance.init_instance  (*args, **kwargs)

                    self._logger.debug ("BOUND bind_adaptor %s.%s -- success"
                                     % (adaptor_name, cpi_cname))
                    return cpi_instance

                else :
                    # the async constructor will return a task, which we pass
                    # back to the caller (instead of the adaptor instance). That 
                    # task is responsible for binding the adaptor to the later 
                    # returned API instance.
                    self._logger.debug ("bind_adaptor %s.%s -- async task creation"  \
                                     % (adaptor_name, cpi_cname))

                    task = cpi_instance.init_instance_async (ttype, *args, **kwargs)
                    return task


            except Exception as e :
                # adaptor class initialization failed - try next one
                m    = "adaptor class ctor failed : %s.%s: %s"  %  (adaptor_name, cpi_class, str(e))
                msg += "\n  %s" % m
                self._logger.info("bind_adaptor %s", m)
                continue

        self._logger.error ("No suitable adaptor found for '%s' and URL scheme '%s'" %  (ctype, schema))
        raise saga.exceptions.NotImplemented ("No suitable adaptor found: %s" %  msg)


    #-----------------------------------------------------------------
    # 
    def loaded_adaptors (self):
        return self._adaptor_registry


    #-----------------------------------------------------------------
    # 
    def _dump (self) :
        import pprint
        pprint.pprint (self._adaptor_registry)

