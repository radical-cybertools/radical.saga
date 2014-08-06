
.. _file_managemen:

Files and Directories
=====================

Introduction
------------

The file managment API provides the ability to interact with (local and
remote) file systems via the two classes, :class:`saga.filesystem.Directory` and
:class:`saga.filesystem.File`. The API provides a number of operations, which all
behave similar to the common unix command line tools (cp, ls, rm etc).

**Example**::

    # get a directory handle
    dir = saga.filesystem.Directory("sftp://localhost/tmp/")
  
    # create a subdir
    dir.make_dir ("data/")
  
    # list contents of the directory
    files = dir.list ()
  
    # copy *.dat files into the subdir
    for f in files :
        if re.match ('^.*\.dat$', f) :
            dir.copy (f, "sftp://localhost/tmp/data/")

The above example covers most of the semantics of the filesystem package --
additional capabilities, such get_size() or move(), can be found in the
individual class documentations.


.. _filesystemflags:

Flags
-----

The following constants are defined as valid flags for file and directory methods:

.. currentmodule:: saga.filesystem
.. data:: OVERWRITE
.. data:: RECURSIVE
.. data:: CREATE
.. data:: CREATE_PARENTS
.. data:: LOCK
.. data:: EXCLUSIVE 
.. data:: DEREFERENCE

.. #############################################################################
.. _file:

File -- :class:`saga.filesystem.File`
----------------------------------------

.. autoclass:: saga.filesystem.File
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
   :exclude-members: create, read, write, seek, read_v, write_v, size_p, read_p, write_p, size_e, read_e, write_e, modes_e, __module__


.. #############################################################################
.. _dir:

Directory -- :class:`saga.filesystem.Directory`
-----------------------------------------------

.. autoclass:: saga.filesystem.Directory
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
   :exclude-members: create, __module__


