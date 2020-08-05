
Part 1: Introduction
********************

The RADICAL-SAGA module provides an object-oriented programming interface for job
submission and management, resource allocation, file handling and coordination
and communication - functionality that is required in the majority of
distributed applications, frameworks and tool.

SAGA encapsulates the complexity and heterogeneity of different distributed
computing systems and 'cyberinfrastructures' by providing a single, coherent API
to the application developer. The so-called adaptor-mechanism that is
transparent to the application translates the API calls to the different
middleware interfaces.  A list of available adaptors can be found in
:ref:`chapter_adaptors`.

In part 2 of this tutorial, we will start with using the local (fork) job
adaptor. In part 3, we use the ssh job adaptor to submit a job to a remote
host. In part 4, we use one of the HPC adaptors (sge, slurm, pbs) to submit a
job to an HPC cluster. Additionally, we introduce the sftp file adaptor  to
implement input and output file staging.


Installation
============

.. warning:: RADICAL-SAGA requires **Python >= 3.6**. It won't work with an older version of Python.

Install Virtualenv
------------------

A small Python command-line tool called `virtualenv <http://www.python.org/>`_
allows you to create a local Python environment (sandbox) in user space, which 
allows you to install additional Python packages without having to be 'root'.

To create your local Python environment run the following command (you can install virtualenv on most systems via apt-get or yum, etc.):

.. code-block:: bash

   virtualenv $HOME/tutorial

If you don't have virtualenv installed and you don't have root access on your machine, you can use the following script instead:

.. code-block:: bash

   curl --insecure -s https://raw.github.com/pypa/virtualenv/master/virtualenv.py | python - $HOME/tutorial

.. note:: If you have multiple Python versions installed on your system, you can use the ``virtualenv --python=PYTHON_EXE`` flag to force virtualenv to use a specific version.

Next, you need to activate your Python environment in order to make it work:

.. code-block:: bash

   source $HOME/tutorial/bin/activate

Activating the virtualenv is very important. If you don't activate your virtualenv, the rest of this tutorial will not work. You can usually tell that your environment is activated properly if your bash command-line prompt starts with ``(tutorial)``.


Install RADICAL-SAGA
--------------------

The latest radical.saga module is available via the `Python Package Index <https://pypi.python.org/pypi/radical.saga>`_  (PyPi). PyPi packages are installed very similar to Linux deb or rpm packages with a tool called ``pip`` (which stands for "pip installs packages"). Pip is installed by default in your virtualenv, so in order to install RADICAL-SAGA, the only thing you have to do is this:

.. code-block:: bash

   pip install radical.saga

To make sure that your installation works, run the following command to check if
the radical.saga module can be imported by the interpreter (the output of the
command below should be version number of the radical.saga module):

.. code-block:: bash

   python -c "import radical.saga as rs; print(rs.version)"

