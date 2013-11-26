
############
Installation
############


Requirements
------------

saga-python has the following requirements:

* Python 2.5 or newer


Installation via PyPi
---------------------

saga-python is available for download via PyPI and may be installed using 
easy_install or pip (preferred). Both automatically download and install all 
dependencies required by saga-python if they can't be found on your system:

.. code-block:: bash

    pip install saga-python


or with easy_install:

.. code-block:: bash

    easy_install saga-python


Using Virtualenv
----------------

If you don't want to (or can't) install SAGA Python into your system's Python 
environment, there's a simple (and often preferred) way to create an 
alternative Python environment (e.g., in your home directory): 

.. code-block:: bash

    virtualenv --no-site-package $HOME/sagenv/
    . $HOME/sagenv/bin/activate
    pip install saga-python   


**What if my system Doesn't come With virtualenv, pip or easy_install?**

There's a simple workaround for that using the 'instant' version of virtualenv. 
It also installs easy_install and pip:

.. code-block:: bash

    curl --insecure -s https://raw.github.com/pypa/virtualenv/1.9.X/virtualenv.py | python - $HOME/sagaenv
    . $HOME/sagaenv/bin/activate
    pip install saga-python


Installing the Latest Development Version
-----------------------------------------

.. warning:: Please keep in mind that the latest development version of SAGA Python can be highly unstable or even completely broken. It's not recommended to use it in a production environment.

You can install the latest development version of SAGA Python directly from our Git repository using pip:

.. code-block:: bash

    pip install -e git://github.com/saga-project/saga-python.git@devel#egg=saga-python
