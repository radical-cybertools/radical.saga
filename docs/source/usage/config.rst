
*************
Configuration
*************

.. note::

   SAGA has been designed as a zero-configuration library. Unless you are 
   experiencing problems with one of the default configuration settings, there's 
   really no need to create a configuration file for SAGA.

SAGA and its individual middleware adaptors provide various optional
:ref:`conf_options`. While SAGA tries to provide sensible default values  for
the majority of these options (zero-conf), it can sometimes be necessary to
modify or extend SAGA's configuration. SAGA provides two ways to access and
modify  its configuration: via :ref:`conf_file` (recommended) and via the
:ref:`conf_api` (for advanced use-cases).

.. _conf_file:

Configuration Files
-------------------

If you need to make persistent changes to any of SAGA's :ref:`conf_options`, the
simplest option is to create a configuration file. During startup, SAGA checks 
for the existence of a configuration file in `$HOME/.saga.conf`.  If that 
configuration file is found, it is parsed by SAGA's configuration system.
SAGA configuration files use a structure that looks like this::

    [saga.engine]
        option = value

    [saga.logger]
        option = value
        
    [saga.adaptor.name]
        option = value


.. _conf_options:

Configuration Options
---------------------

.. warning:: This should be generated automatically!


.. _conf_api:

Configuration API
-----------------

Module saga.utils.config
*************************

The config module provides classes and functions to introspect and modify
SAGA's configuration. The :func:`getConfig` function is used to get the
:class:`GlobalConfig` object which represents the current configuration 
of SAGA::

   from saga.utils.config import getConfig 

   sagaconf = getConfig()
   print sagaconf.get_category('saga.utils.logger')

.. automodule:: saga.utils.config
   :members:


