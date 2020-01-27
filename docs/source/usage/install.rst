
############
Installation
############


Requirements
------------

radical.saga has the following requirements:

* Python 3.5 or newer


Installation via PyPi
---------------------

radical.saga is available for download via PyPI and may be installed using
easy_install or pip (preferred). Both automatically download and install all
dependencies required by radical.saga if they can't be found on your system:

.. code-block:: bash

    pip install radical.saga



Using Virtualenv
----------------

If you don't want to (or can't) install RADICAL-SAGA into your system's Python
environment, there's a simple (and often preferred) way to create an
alternative Python environment (e.g., in your home directory):

.. code-block:: bash

    virtualenv --no-site-package $HOME/sagaenv/
    . $HOME/sagaenv/bin/activate
    pip install radical.saga


**What if my system Doesn't come With virtualenv, pip or easy_install?**

There's a simple workaround for that using the 'instant' version of virtualenv.
It also installs easy_install and pip:

.. code-block:: bash

    wget https://raw.githubusercontent.com/pypa/virtualenv/1.9.1/virtualenv.py
    python virtualenv.py $HOME/sagaenv/ --no-site-packages
    . $HOME/sagaenv/bin/activate
    pip install radical.saga


Installing the Latest Development Version
-----------------------------------------

.. warning:: Please keep in mind that the latest development version of RADICAL-SAGA can be highly unstable or even completely broken. It's not recommended to use it in a production environment.

You can install the latest development version of RADICAL-SAGA directly from our Git repository using pip:

.. code-block:: bash

    pip install -e git://github.com/radical-cybertools/radical.saga.git@devel#egg=radical.saga

