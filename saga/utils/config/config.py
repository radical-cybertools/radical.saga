
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


''' Provides API handles for SAGA's configuration system.
'''

import os

from saga.utils.singleton  import Singleton
from configfile            import ConfigFileReader

################################################################################
##
def getConfig():
    """ Returns a handle to the global configuration object.
    """
    return Configuration() 

################################################################################
##
class ConfigOption(object):
    """ Represent a (mutable) configuration option.
    """

    def __init__(self, category, name, val_type, default_val, valid_options,
                 documentation, env_var):

        self._category = category
        self._name = name
        self._val_type = val_type
        self._default_val = default_val
        self._valid_options = valid_options
        self._env_var = env_var
        self._documentation = documentation
        self._value = None

    def __str__(self):
        return str({'name':self._name, 'value':self._value})

    def as_dict(self):
        return {self._name:self._value}

    @property
    def category(self):
        return self._category

    def set_value(self, value):
        # make sure we got the right value type
        if type(value) != self._val_type:
            raise ValueTypeError(self._category, self._name, 
              type(value), self._val_type)

        self._value = value

    def get_value(self):
        return self._value

################################################################################
##
class Configuration(object): 
    """ Represents SAGA's global configuration.

        The Configuration class can be used to introspect and modify the
        configuration options for SAGA and its various middleware adaptors. 
        It is a 'Singleton' object, which means that  multiple instances all 
        point to the same object which holds the global configuration.
    """    
    __metaclass__ = Singleton

    def __init__(self):

        # _all_valid_options contains the raw option sets
        # added to configuration by 'Configurable' classes
        self._all_valid_options = list()

        # _master config holds the parsed options, including 
        # values fetched from configuartion files and env variables
        self._master_config = dict()

        self._initialize()

    def _update(self, namespace, valid_options):
        # add the new options to the global dictionary
        self._all_valid_options += valid_options
        # and re-_initialize the configuration object.
        self._initialize()

    def _initialize(self, inject_cfg_file=None, add_cfg_file=None):
        """ Initialize the global configuration.

            :param inject_cfg_file: is used *only* for testing purposes 
             and overwrites / ignores the regular config file locations 
             /etc/saga.cfg & $HOME/.saga.cfg

            :param add_cfg_file: is used *only* for testing purposes 
             and adds a specific config file to the list of evaluated files.  
        """
        cfg_files = list()
        if inject_cfg_file is not None:
            cfg_files.append(inject_cfg_file)
        else:
            # check for the existence of regular configuration files
            sys_cfg = '/etc/saga.cfg'
            if os.path.exists(sys_cfg):
                cfg_files.append(sys_cfg)
            usr_cfg = '%s/.saga.cfg' % os.path.expanduser("~")
            if os.path.exists(usr_cfg):
                cfg_files.append(usr_cfg)

        if add_cfg_file is not None:
            cfg_files.append(add_cfg_file)

        cfr = ConfigFileReader(cfg_files)
    
        # cfg_file_dict holds all configuration variables that 
        # were read from either a system-wide or user configuration file.
        cfg_file_dict = cfr.get_config_dict()

        # load valid options and add them to the configuration
        for option in self._all_valid_options:
            cat = option['category']
            if cat not in self._master_config:
                # first occurrence - add new category key
                self._master_config[cat] = dict()

            ev = None
            if option['env_variable'] :
                ev = os.environ.get(option['env_variable'])

            if (option['category'] in cfg_file_dict) and \
                option['name']     in cfg_file_dict[option['category']]:
                # found entry in configuration file -- use it
                tmp_value = cfg_file_dict[option['category']][option['name']]
                # some list types need type conversion
                if option['type'] == list:
                    value = tmp_value.split(",")
                elif option['type'] == bool:
                    if tmp_value.lower() == 'true':
                      value = True
                    elif tmp_value.lower() == 'false':
                      value = False 
                    else:
                      raise ValueTypeError(option['category'], option['name'],
                          tmp_value, option['type'])
                else:
                    value = tmp_value

            elif ev is not None:
                #getLogger('engine').debug("Using environment variable '%s' to set config option '%s.%s' to '%s'." \
                #    % (option['env_variable'], option['category'], option['name'], ev))
                tmp_value = ev
                if option['type'] == list:
                    value = tmp_value.split(",")
                elif option['type'] == bool:
                    if tmp_value.lower() == 'true':
                      value = True
                    elif tmp_value.lower() == 'false':
                      value = False 
                    elif tmp_value == '1':
                      value = True
                    elif tmp_value == '0':
                      value = False
                    else:
                      raise ValueTypeError(option['category'], option['name'],
                          tmp_value, option['type'])
                else:
                    value = tmp_value
            else:
                value = option['default']

            if not 'valid_options' in option :
                option['valid_options'] = None

            self._master_config[cat][option['name']] = ConfigOption(
                option['category'],
                option['name'],
                option['type'],
                option['default'],
                option['valid_options'],
                option['documentation'],
                option['env_variable'])

            self._master_config[cat][option['name']].set_value(value) 


        # now walk through the cfg_file_dict -- for all entries not yet handled
        # (i.e. which have not been registered before), use default
        # ConfigOptions:
        #       category       : from cfg_file_dict
        #       name           : from cfg_file_dict
        #       value          : from cfg_file_dict
        #       type           : string
        #       default        : ""
        #       valid_options  : None
        #       documentation  : ""
        #       env_variable   : None
        #
        # If later initialize_ is called again, and that option has been
        # registered meanwhile, this entry will be overwritten.

        for section in cfg_file_dict.keys () :
            for name in cfg_file_dict[section].keys () :
                value = cfg_file_dict[section][name]

                # check if this is a registered entry
                exists = False
                if section in self._master_config :
                    if name in self._master_config[section] :
                        exists = True
                else :
                    # section does not exist - create it, and add option
                    self._master_config[section] = {}

                # if not, add it with default ConfigOptions
                if not exists :
                    self._master_config[section][name] = ConfigOption(
                        section   , # category
                        name      , # name
                        str       , # type
                        ""        , # default
                        None      , # valid_options
                        ""        , # documentation
                        None      ) # env_variable

                    self._master_config[section][name].set_value(value)


        # next, we need to parse adaptor se

    def has_category(self, category_name):
        """ Check for a specific configuration category.
        """
        if category_name not in self._master_config:
            return False
        else:
            return True

    def get_category(self, category_name):
        """ Return a specific configuration category.
        """
        if category_name not in self._master_config:
            raise CategoryNotFound(category_name)
        else:
            return self._master_config[category_name]

    def has_option(self, category_name, option_name):
        if category_name not in self._master_config:
            return False
        else:
            if option_name not in self._master_config[category_name]:
                return False
            else:
                return True


    def get_option(self, category_name, option_name):
        if category_name not in self._master_config:
            raise CategoryNotFound(category_name)
        else:
            if option_name not in self._master_config[category_name]:
                raise OptionNotFound(category_name, option_name)
            else:
                return self._master_config[category_name][option_name]


    def as_dict (self, cn = None) :

        ret = {}

        if  cn : 
            for on in self._master_config[cn] :
                ret[on] = self.get_option (cn, on).get_value ()

        else :
            for cn in self._master_config :
                ret[cn] = {}
  
                for on in self._master_config[cn] :
                    ret[cn][on] = self.get_option (cn, on).get_value ()

        return ret



################################################################################
##
class Configurable(object):
    """ This class provides an interface for all configurable 
        SAGA objects.
    """
    def __init__(self, namespace, valid_options):
        self._namespace = namespace
        self._valid_options = valid_options

        ## sanity check for valid_options

        ## register a new 'configurable' object
        getConfig()._update(namespace, self._valid_options)

    def get_config(self):
        return getConfig().get_category(self._namespace)


################################################################################
##
class CategoryNotFound(Exception):
    def __init__(self, name):
        msg = "A category with name '%s' could not be found." % name
        Exception.__init__(self, msg)

class OptionNotFound(Exception):
    def __init__(self, category_name, option_name):
        name = "%s.%s" % (category_name, option_name)
        msg  = "An option with name '%s' could not be found." % (name)
        Exception.__init__(self, msg)

class ValueTypeError(Exception):
    def __init__(self, category_name, option_name, value_type, required_type):
        name = "%s.%s" % (category_name, option_name)
        msg  = "Option %s requires value of type '%s' but got '%s'." \
             % (name, required_type, value_type)
        Exception.__init__ (self, msg)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

