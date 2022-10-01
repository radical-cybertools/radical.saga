
.. _namespaces:

Namespaces
==========

Introduction
------------

Namespaces are an abstraction over firlesystem and other hirarchical constructs
which have a notion of a `:class:`saga.namespace.Directory` and of
`:class:`saga.namespace.Entry`s which exist in those directories.  The API
provides a number of operations, which all behave similar to the common unix
command line tools (cp, ls, rm etc).

**Example**::

    # get a directory handle
    dir = radical.saga.namespace.Directory("sftp://localhost/tmp/")

    # create a subdir
    dir.make_dir ("data/")

    # list contents of the directory
    entries = dir.list ()

    # copy *.dat files into the subdir
    for e in entries :
        if re.match ('^.*\.dat$', f) :
            dir.copy (e, "sftp://localhost/tmp/data/")

The above example covers most of the semantics of the namespace package --
additional capabilities, such get_size() or move(), can be found in the
individual class documentations.


.. _namespaceflags:

Flags
-----

The following constants are defined as valid flags for file and directory methods:

.. currentmodule:: radical.saga.namespace
.. data:: OVERWRITE
.. data:: RECURSIVE
.. data:: CREATE
.. data:: CREATE_PARENTS
.. data:: LOCK
.. data:: EXCLUSIVE
.. data:: DEREFERENCE

.. #############################################################################
.. _namespace:

Entry -- :class:`radical.saga.namespace.Entry`
---------------------------------------------

.. autoclass:: radical.saga.namespace.Entry
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
   :exclude-members: create, __module__


.. #############################################################################
.. _dir:

Directory -- :class:`radical.saga.namespace.Directory`
-------------------------------------------------------

.. autoclass:: radical.saga.namespace.Directory
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
   :exclude-members: create, __module__


