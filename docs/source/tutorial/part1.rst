
Part 1: Introduction
********************

The SAGA Python module provides an object-oriented programming interface for job
submission and management, resource allocation, file handling and coordination
and communication - functionality that is required in the majority of
distributed applications, frameworks and tool.

The big picture looks like this, but as an application developer you don't have to worry about most of it:

SAGA encapsulates the complexity and heterogeneity of different distributed
computing systems and 'cyberinfrastructures' by providing a single, coherent API
to the application developer. A plug-in mechanism that is transparent to the
application translates the API calls to the different middleware interfaces. 
A list of available adaptors can be found in :ref:`chapter_adaptors`.
