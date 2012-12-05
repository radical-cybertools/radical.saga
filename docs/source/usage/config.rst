#############
Configuration
#############

.. note::

   SAGA has been designed as a zero-configuration library. Unless you are 
   dissatisfied with any of the default configuration settings there's no need  
   to create a configuration file for SAGA.

SAGA as well as its individual middleware adaptors provide various optional
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
in two different locations for the existence of a configuration file:

- ``/etc/saga.conf`` - for a system-wide configuration
- ``$HOME/.saga.conf`` - for a user-specific configuration (Note: it start with a '.')

If a configuration file is found, it is parsed by SAGA's configuration system.
If files are present in both locations, SAGA will try to merge both, with the
user-level  configuration (``$HOME/.saga.conf``) always having precedence over
the  system-wide configuration (``$HOME/.saga.conf``). SAGA configuration files 
use a structure that looks like this::

    [saga.core]
        option = value

    [saga.logging]
        option = value
        
    [saga.adaptor.name]
        option = value


.. _conf_options:

Configuration Options
---------------------

This should be GENERATED


.. _conf_api:

Configuration API
-----------------

This should be GENERATED