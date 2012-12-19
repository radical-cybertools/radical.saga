# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' SAGA Setup script.
'''

import os
import sys
import shutil
import fileinput

from distutils.core import setup
from distutils.command.install_data import install_data
from distutils.command.sdist import sdist

from saga import version

scripts = [] # ["bin/bliss-run"]

import sys
if sys.hexversion < 0x02050000:
    raise RuntimeError, "SAGA requires Python 2.5 or higher"

class our_install_data(install_data):

    def finalize_options(self):
        self.set_undefined_options('install',
            ('install_lib', 'install_dir'),
        )
        install_data.finalize_options(self)

    def run(self):
        install_data.run(self)
        # ensure there's a bliss/VERSION file
        fn = os.path.join(self.install_dir, 'saga', 'VERSION')
        open(fn, 'w').write(version)
        self.outfiles.append(fn)

class our_sdist(sdist):

    def make_release_tree(self, base_dir, files):
        sdist.make_release_tree(self, base_dir, files)
        # ensure there's a air/VERSION file
        fn = os.path.join(base_dir, 'saga', 'VERSION')
        open(fn, 'w').write(version)

setup_args = {
    'name': "saga",
    'version': version,
    'description': "A native Python implementation of the OGF SAGA standard (GFD.90).",
    'long_description': "SAGA-Python (a.k.a bliss) is a pragmatic and light-weight implementation of the OGF GFD.90 SAGA standard. SAGA-Python is written 100% in Python and focuses on usability and ease of deployment.",
    'author': "Ole Christian Weidner, et al.",
    'author_email': "ole.weidner@rutgers.edu",
    'maintainer': "Ole Christian Weidner",
    'maintainer_email': "ole.weidner@rutgers.edu",
    'url': "http://saga-project.github.com/saga-python/",
    'license': "MIT",
    'classifiers': [
        'Development Status :: 5 - Production/Stable',
        'Environment :: No Input/Output (Daemon)',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'License :: OSI Approved :: MIT License',
        'Topic :: System :: Distributed Computing',
        'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: AIX',
        'Operating System :: POSIX :: BSD',
        'Operating System :: POSIX :: BSD :: BSD/OS',
        'Operating System :: POSIX :: BSD :: FreeBSD',
        'Operating System :: POSIX :: BSD :: NetBSD',
        'Operating System :: POSIX :: BSD :: OpenBSD',
        'Operating System :: POSIX :: GNU Hurd',
        'Operating System :: POSIX :: HP-UX',
        'Operating System :: POSIX :: IRIX',
        'Operating System :: POSIX :: Linux',
        'Operating System :: POSIX :: Other',
        'Operating System :: POSIX :: SCO',
        'Operating System :: POSIX :: SunOS/Solaris',
        'Operating System :: Unix'
        ],

    'packages': [
        "saga",
        "saga.job",
        "saga.filesystem",
        "saga.cpi",
        "saga.cpi.job",
        "saga.cpi.filesystem",
        "saga.adaptors",
        "saga.adaptors.localjob",
        "saga.engine",
        "saga.contrib",
        "saga.utils",
        "saga.utils.cmdlinewrapper",
        "saga.utils.logger",
        "saga.utils.job"
    ],
    'scripts': scripts,
    # mention data_files, even if empty, so install_data is called and
    # VERSION gets copied
    'data_files': [("saga", [])],
    'cmdclass': {
        'install_data': our_install_data,
        'sdist': our_sdist
        }
    }

# set zip_safe to false to force Windows installs to always unpack eggs
# into directories, which seems to work better --
# see http://buildbot.net/trac/ticket/907
if sys.platform == "win32":
    setup_args['zip_safe'] = False

try:
    # If setuptools is installed, then we'll add setuptools-specific arguments
    # to the setup args.
    import setuptools #@UnusedImport
except ImportError:
    pass
else:
    setup_args['install_requires'] = [
        'pexpect', 'colorama'
    ]
    

    if os.getenv('SAGA_NO_INSTALL_REQS'):
        setup_args['install_requires'] = None

##
## PROCESS SETUP OPTIONS FOR DIFFERENT BACKENDS
##

# process AIR_AMQP_HOSTNAME and AIR_AMQP_PORT
#air_amqp_hostname = os.getenv('AIR_AMQP_HOST')
#air_amqp_port = os.getenv('AIR_AMQP_PORT')
#
#if not air_amqp_hostname:
#   air_amqp_hostname = "localhost"
#
#print "setting default amqp hostname to '%s' in air/scripts/config.py" % air_amqp_hostname
#
#if not air_amqp_port:
#   air_amqp_port = "5672"
#
#print "setting default amqp port to '%s' in air/scripts/config.py" % air_amqp_port
#
#
#shutil.copyfile("./air/scripts/config.py.in", "./air/scripts/config.py")


#s = open("./air/scripts/config.py.in").read()
#s = s.replace('###REPLACE_WITH_AMQP_HOSTNAME###', str(air_amqp_hostname))
#s = s.replace('###REPLACE_WITH_AMQP_PORT###', str(air_amqp_port))
#f = open("./air/scripts/config.py", 'w')
#f.write(s)
#f.close()


setup(**setup_args)
