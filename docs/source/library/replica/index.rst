.. _replica_management:

Replica Management
==================

The replica management module provides an interface to 
(distributed) data replication services, like for example iRODS.

The basic usage of the replica module is as follows::

    myfile = saga.replica.LogicalFile("irods://localhost/"+TEMP_FILENAME)
    myfile.add_location("irods:////data/cache/AGLT2_CE_2_FTPplaceholder/whatever?resource=AGLT2_CE_2_FTP")

    mydir = saga.replica.LogicalDirectory("irods://localhost/" + IRODS_DIRECTORY) 
    mydir.make_dir("anotherdir")

Like all SAGA modules, the replica module relies on  middleware adaptors 
to provide bindings to a specific resource manager. Adaptors are implicitly 
selected via the `scheme` part of the URL, e.g., ``local://`` in the example 
above selects the `local` replica adaptor.

.. note:: A list of available adaptors and supported resource managers can be 
          found in the :ref:`chapter_middleware_adaptors` part of this 
          documentation.

The rest of this section is structured as follows:

.. contents:: Table of Contents
   :local:

.. #############################################################################
.. _filesystemflags:

Flags
-----

The following constants are defined as valid flags for logical file and directory methods:

.. currentmodule:: saga.filesystem
.. data:: OVERWRITE
.. data:: RECURSIVE
.. data:: CREATE
.. data:: CREATE_PARENTS
.. data:: LOCK
.. data:: EXCLUSIVE 
.. data:: DEREFERENCE

.. #############################################################################
.. _replica_file:

Logical File -- :class:`saga.replica.LogicalFile`
----------------------------------------------------

.. autoclass:: saga.replica.LogicalFile
   :members:
   :undoc-members:
   :special-members: __init__
   :exclude-members: create, __module__


.. #############################################################################
.. _replica_directory:

Logical Directory -- :class:`saga.replica.LogicalDirectory`
---------------------------------------------------------

.. autoclass:: saga.replica.LogicalDirectory
   :members:
   :undoc-members:
   :special-members: __init__
   :exclude-members: create, __module__
