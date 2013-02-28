.. _advert_management:

Advert Management
=================

SAGA's advert module supports the coordination of distributed application
components, by exposing and sharing application specific pieces of information
between application instances, and by creating and delivering notifications on
changes of those information.

The basic usage of the advert module is as follows::

  class my_cb (saga.Callback) :
  
    def cb (self, obj, key, val) :
      print " ----------- callback triggered for %s - %s - %s" % (obj, key, val)
      return True
  
  
  ad = saga.advert.Directory ("redis://redishost.net/tmp/myapp/%d" % os.getpid (),
                                  saga.filesystem.CREATE | saga.filesystem.CREATE_PARENTS)
  ad.set_attribute ('start',      time.time())
  ad.set_attribute ('user',       os.get_uid())
  ad.set_attribute ('iteration',  0)
    
  ad.add_callback  ('iteration', my_cb ())
  ad.set_attribute ('iteration', 1)
  

.. seealso:: More examples on how to use the SAGA advert module can be found in 
             the :ref:`code_examples_advert` section of the 
             :ref:`chapter_code_examples` chapter.

Like all SAGA modules, the advert module relies on  middleware adaptors 
to provide bindings to a specific backend system. Adaptors are implicitly 
selected via the `scheme` part of the URL, e.g., ``redis://`` in the example 
above selects the `redis` backend.

.. note:: A list of available adaptors and supported backends can be 
          found in the :ref:`chapter_middleware_adaptors` part of this 
          documentation.

The rest of this section is structured as follows:

.. contents:: Table of Contents
   :local:

.. #############################################################################

.. autoclass:: saga.advert.Entry
   :members:
   :inherited-members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: saga.advert.Directory
   :members:
   :inherited-members:
   :undoc-members:
   :show-inheritance:

