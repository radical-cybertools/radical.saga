
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Provides the SAGA runtime. """

import radical.utils as ru

from ..        import exceptions as rse
from ..version import *


# ------------------------------------------------------------------------------
#
# we set the default of the pty share mode to 'no' on CentOS, as that seems to
# consistently come with old ssh versions which can't handle sharing for sftp
# channels.
#
_share_mode_default = 'auto'
try:
    import subprocess as sp
    _p = sp.Popen ('lsb_release -a | grep "Distributor ID" | cut -f 2 -d ":"',
                   stdout=sp.PIPE, stderr=sp.STDOUT, shell=True)
    _os_flavor = _p.communicate()[0].strip().lower()

    if 'centos'  in _os_flavor or \
       'cent_os' in _os_flavor or \
       'cent-os' in _os_flavor or \
       'cent os' in _os_flavor :
        _share_mode_default = 'no'

except Exception:
    # we ignore this then -- we are relatively sure that the above should work
    # on CentOS...
    pass


# ------------------------------------------------------------------------------
#
class Engine(object, metaclass=ru.Singleton):
    """
    Represents the SAGA engine runtime system.

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
          'type'    : 'radical.saga.job.Job',
          'class'   : 'LocalJob',
          },
          {
          'type'    : 'radical.saga.job.Service',
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

          for (adaptor, adaptor_class) in \
                  self._engine._adaptor_registry{'job'}{url.scheme}

              try :
                  self._adaptor = adaptor_class(self, url, session}

              except saga.Exception e :
                  # adaptor bailed out
                  continue

              else :
                  # successfully bound to adaptor
                  return
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        # Engine manages cpis from adaptors
        self._adaptor_registry = dict()

        # get angine, adaptor and pty configs
        self._cfg      = ru.Config('radical.saga.engine')
        self._pty_cfg  = ru.Config('radical.saga.pty')
        self._registry = ru.Config('radical.saga.registry')

        # Initialize the logging, and log version (this is a singleton!)
        self._logger = ru.Logger('radical.saga')
        self._logger.info('radical.saga         version: %s' % version_detail)

        # load adaptors
        self._load_adaptors()


    # --------------------------------------------------------------------------
    #
    def _load_adaptors (self, inject_registry=None):
        """
        Try to load all adaptors that are registered in saga.engine.registry.py.
        This method is called from the constructor.  As Engine is a singleton,
        this method is called once after the module is first loaded in any
        python application.

        :param inject_registry: Inject a fake registry. *For unit tests only*.
        """


        self._logger.debug ("listing  adaptor registry: %s" % self._registry)

        # check if some unit test wants to use a special registry.  If
        # so, we reset cpi infos from the earlier singleton creation.
        if inject_registry is not None:
            self._adaptor_registry = dict()
            self._registry = {'adaptor_registry' : inject_registry}

        # attempt to load all registered modules
        for module_name in self._registry.get('adaptor_registry', []):

            self._logger.info ("loading  adaptor %s" % module_name)


            # first, import the module
            adaptor_module = None
            try :
                adaptor_module = ru.import_module(module_name)

            except Exception as e:
                self._logger.warning("skip adaptor %s: import failed (%s)",
                                     module_name, e, exc_info=True)
                continue

            # we expect the module to have an 'Adaptor' class
            # implemented, which, on calling 'register()', returns
            # a info dict for all implemented adaptor classes.
            adaptor_instance = None
            adaptor_info     = None

            try:
                adaptor_instance = adaptor_module.Adaptor ()
                adaptor_info     = adaptor_instance.register ()

            except rse.SagaException:
                self._logger.warning("skip adaptor %s: failed to load",
                                     module_name, exc_info=True)
                continue

            except Exception:
                self._logger.warning("skip adaptor %s: init failed",
                                     module_name, exc_info=True)
                continue


            # the adaptor must also provide a sanity_check() method, which sould
            # be used to confirm that the adaptor can function properly in the
            # current runtime environment (e.g., that all pre-requisites and
            # system dependencies are met).
            try:
                adaptor_instance.sanity_check ()

            except Exception:
                self._logger.warning("skip adaptor %s: test failed",
                                     module_name, exc_info=True)
                continue


            # check if we have a valid adaptor_info
            if adaptor_info is None :
                self._logger.warning("skip adaptor %s: invalid adaptor data",
                                     module_name)
                continue


            if  'name'    not in adaptor_info or \
                'cpis'    not in adaptor_info or \
                'version' not in adaptor_info or \
                'schemas' not in adaptor_info    :
                self._logger.warning("skip adaptor %s: incomplete data",
                                     module_name)
                continue


            adaptor_name    = adaptor_info['name']
            adaptor_version = adaptor_info['version']
            adaptor_schemas = adaptor_info['schemas']
            adaptor_enabled = True  # default

            # disable adaptors in 'alpha' or 'beta' versions -- unless
            # the 'load_beta_adaptors' config option is set to True
            if not self._cfg.load_beta_adaptors:

                if 'alpha' in adaptor_version.lower() or \
                   'beta'  in adaptor_version.lower()    :

                    self._logger.warning("skip beta adaptor %s (version %s)",
                                         module_name, adaptor_version)
                    continue


            # get the 'enabled' option in the adaptor's config
            # section (radical.saga.cpi.base) ensures that the option exists,
            # if it is initialized correctly in the adaptor class.
            adaptor_config  = None
            adaptor_enabled = False

            try :
                adaptor_config  = ru.Config('radical.saga.adaptors',
                                            name=adaptor_name)
                adaptor_enabled = adaptor_config.get('enabled', True)

            except rse.SagaException:
                self._logger.warning("skip adaptor %s: init failed",
                                     module_name, exc_info=True)
                continue

            except Exception as e:
                self._logger.warning("skip adaptor %s: init error",
                                     module_name, exc_info=True)
                continue


            # only load adaptor if it is not disabled via config files
            if not adaptor_enabled:
                self._logger.warning("skip adaptor %s: disabled", module_name)
                continue


            # check if the adaptor has anything to register
            if 0 == len (adaptor_info['cpis']) :
                self._logger.warning("skip adaptor %s: adaptor has no cpis",
                                     module_name)
                continue


            # we got an enabled adaptor with valid info - yay!  We can
            # now register all adaptor classes (cpi implementations).
            for cpi_info in adaptor_info['cpis'] :

                # check cpi information details for completeness
                if  'type'  not in cpi_info or \
                    'class' not in cpi_info    :
                    self._logger.warning("skip %s cpi: incomplete info detail",
                                         module_name)
                    continue


                # adaptor classes are registered for specific API types.
                cpi_type  = cpi_info['type']
                cpi_cname = cpi_info['class']
                cpi_class = None

                try :
                    cpi_class = getattr (adaptor_module, cpi_cname)

                except Exception:
                    # this exception likely means that the adaptor does not call
                    # the radical.saga.adaptors.Base initializer (correctly)
                    self._logger.warning("skip adaptor %s: invalid %s",
                                         module_name, cpi_info['class'],
                                         exc_info=True)
                    continue

                # make sure the cpi class is a valid cpi for the given type.
                # We walk through the list of known modules, and try to find
                # a modules which could have that class.  We do the following
                # tests:
                #
                #   cpi_class: ShellJobService
                #   cpi_type:  radical.saga.job.Service
                #   modules:   radical.saga.adaptors.cpi.job
                #   modules:   radical.saga.adaptors.cpi.job.service
                #   classes:   radical.saga.adaptors.cpi.job.Service
                #   classes:   radical.saga.adaptors.cpi.job.service.Service
                #
                #   cpi_class: X509Context
                #   cpi_type:  radical.saga.Context
                #   modules:   radical.saga.adaptors.cpi.context
                #   classes:   radical.saga.adaptors.cpi.context.Context
                #
                # So, we add a 'adaptors.cpi' after the 'saga' namespace
                # element, then append the rest of the given namespace.  If that
                # gives a module which has the requested class, fine -- if not,
                # we add a lower cased version of the class name as last
                # namespace element, and check again.

                # ->   radical .  saga .  job .  Service
                # <- ['radical', 'saga', 'job', 'Service']
                cpi_type_nselems = cpi_type.split ('.')

                if  len(cpi_type_nselems) < 3 or \
                    len(cpi_type_nselems) > 4    :
                    self._logger.warning("skip adaptor %s invalid cpi %s",
                                         module_name, cpi_type)
                    continue

                if  cpi_type_nselems[0] != 'radical' and \
                    cpi_type_nselems[1] != 'saga'    :
                    self._logger.warning("skip adaptor %s: invalid cpi ns %s",
                                         module_name, cpi_type, exc_info=True)
                    continue

                # -> ['radical', 'saga',                    'job', 'Service']
                # <- ['radical', 'saga', 'adaptors', 'cpi', 'job', 'Service']
                cpi_type_nselems.insert (2, 'adaptors')
                cpi_type_nselems.insert (3, 'cpi')

             #  # -> ['radical', 'saga', 'adaptors', 'cpi', 'job',  'Service']
             #  # <- ['radical', 'saga', 'adaptors', 'cpi', 'job'], 'Service'
             #  cpi_type_cname = cpi_type_nselems.pop ()
             #
             #  # -> ['radical', 'saga', 'adaptors', 'cpi', 'job'], 'Service'
             #  # <-  'radical.saga.adaptors.cpi.job
             #  # <-  'radical.saga.adaptors.cpi.job.service
             #  cpi_type_modname_1 = '.'.join (cpi_type_nselems)
             #  cpi_type_modname_2 = '.'.join (cpi_type_nselems + \
             #                                 [cpi_type_cname.lower()])
             #
             #  # does either module exist?
             #  cpi_type_modname = None
             #
             #  if  cpi_type_modname_1 in sys.modules :
             #      cpi_type_modname = cpi_type_modname_1
             #
             #  if  cpi_type_modname_2 in sys.modules :
             #      cpi_type_modname = cpi_type_modname_2
             #
             #  if  not cpi_type_modname :
             #      self._logger.warning("skip adaptor %s: unknown cpi %s",
             #                           module_name, cpi_type, exc_info=True)
             #      sys.exit()
             #      continue
             #
             #  # so, make sure the given cpi is actually
             #  # implemented by the adaptor class
             #  cpi_ok = False
             #  for name, cpi_obj \
             #      in inspect.getmembers (sys.modules[cpi_type_modname]):
             #      if  name == cpi_type_cname      and \
             #          inspect.isclass (cpi_obj)       :
             #          if  issubclass (cpi_class, cpi_obj) :
             #              cpi_ok = True
             #
             #  if not cpi_ok :
             #      self._logger.warning("skip adaptor %s: no cpi %s (%s)",
             #                           module_name, cpi_class, cpi_type,
             #                            exc_info=True)
             #      continue


                # finally, register the cpi for all its schemas!
                registered_schemas = list()
                for adaptor_schema in adaptor_schemas:

                    adaptor_schema = adaptor_schema.lower ()

                    # make sure we can register that cpi type
                    if cpi_type not in self._adaptor_registry:
                        self._adaptor_registry[cpi_type] = dict()

                    # make sure we can register that schema
                    if adaptor_schema not in self._adaptor_registry[cpi_type]:
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
                    if info in self._adaptor_registry[cpi_type][adaptor_schema]:
                        self._logger.warning("skip adaptor %s: exists %s: %s",
                                             module_name, cpi_class,
                                             adaptor_instance, exc_info=True)
                        continue

                    self._adaptor_registry[cpi_type] \
                                          [adaptor_schema].append(info)
                    registered_schemas.append(str("%s://" % adaptor_schema))

                self._logger.info("Register adaptor %s for %s API: %s" %
                                 (module_name, cpi_type, registered_schemas))



    # --------------------------------------------------------------------------
    #
    def find_adaptors (self, ctype, schema) :
        ''' Look for a suitable cpi class serving a particular schema

            This method will sift through our adaptor registry (see
            '_load_adaptors()', and dig for any adaptor which marches the given
            api class type and schema.  All matching adaptors are returned (by
            name)
        '''

        if ctype not in self._adaptor_registry :
            return []

        if not schema.lower () in self._adaptor_registry[ctype] :
            return []

        adaptor_names = []
        for info in self._adaptor_registry[ctype][schema.lower ()] :
            adaptor_names.append (info['adaptor_name'])

        return adaptor_names



    # --------------------------------------------------------------------------
    #
    def get_adaptor (self, adaptor_name) :
        ''' Return the adaptor module's ``Adaptor`` class for the given adaptor
            name.

            This method is used if adaptor or API object implementation need to
            interact with other adaptors.
        '''

        for ctype in list(self._adaptor_registry.keys ()) :
            for schema in list(self._adaptor_registry[ctype].keys ()) :
                for info in self._adaptor_registry[ctype][schema] :
                    if info['adaptor_name'] == adaptor_name:
                        return info['adaptor_instance']

        error_msg = "No adaptor named '%s' found" % adaptor_name
        self._logger.error(error_msg)
        raise rse.NoSuccess(error_msg)


    # --------------------------------------------------------------------------
    #
    def bind_adaptor (self, api_instance, ctype, schema,
                      preferred_adaptor, *args, **kwargs) :
        '''
        Look for a suitable adaptor class to bind to, instantiate it, and
        initialize it.

        If 'preferred_adaptor' is not 'None', only that given adaptors is
        considered, and adaptor classes are only created from that specific
        adaptor.
        '''

        if ctype not in self._adaptor_registry:
            error_msg = "No adaptor found for '%s' and URL scheme %s://" \
                                  % (ctype, schema)
            self._logger.error(error_msg)
            raise rse.NotImplemented(error_msg)

        if schema not in self._adaptor_registry[ctype]:
            error_msg = "No adaptor found for '%s' and URL scheme %s://" \
                                  % (ctype, schema)
            self._logger.error(error_msg)
            raise rse.NotImplemented(error_msg)


        # cycle through all applicable adaptors, and try to instantiate
        # a matching one.
        exception = rse.NoSuccess ("binding adaptor failed", api_instance)
        for info in self._adaptor_registry[ctype][schema] :

            cpi_cname        = info['cpi_cname']
            cpi_class        = info['cpi_class']
            adaptor_name     = info['adaptor_name']
            adaptor_instance = info['adaptor_instance']

            try :

                # is this adaptor acceptable?
                if  preferred_adaptor is not None     and \
                    preferred_adaptor != adaptor_instance :

                    # ignore this adaptor
                    self._logger.debug ("bind_adaptor for %s : %s != %s ignored"
                             % (cpi_cname, preferred_adaptor, adaptor_instance))
                    continue


                # instantiate cpi
                cpi_instance = cpi_class (api_instance, adaptor_instance)

              # self._logger.debug("Successfully bound %s.%s to %s" \
              #                  % (adaptor_name, cpi_cname, api_instance))
                return cpi_instance


            except rse.SagaException as e :
                # adaptor class initialization failed - try next one
                exception._add_exception (e)
                self._logger.info("adaptor ctor failed : %s.%s: %s"
                                 % (adaptor_name, cpi_class, str(e)))
                continue
            except Exception as e :
                exception._add_exception (rse.NoSuccess (str(e), api_instance))
                self._logger.info("adaptor ctor failed : %s.%s: %s"
                                 % (adaptor_name, cpi_class, str(e)))
                continue


        self._logger.error("No adaptor found for '%s' and URL scheme '%s'"
                          % (ctype, schema))
        self._logger.info  ("%s" %  (str(exception)))
        raise exception._get_exception_stack ()


    # -----------------------------------------------------------------
    #
    def loaded_adaptors (self):
        return self._adaptor_registry


    # -----------------------------------------------------------------
    #
    def _dump(self):

        import pprint

        print('adaptors')
        pprint.pprint (self._adaptor_registry)




