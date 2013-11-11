
__author__    = "Andre Merzky, Ashley Z, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Setup script. Used by easy_install and pip.
"""

import os
import sys

from setuptools import setup, Command
from distutils.command.install_data import install_data
from distutils.command.sdist import sdist

#-----------------------------------------------------------------------------
# figure out the current version
def update_version():

    version = "latest"

    try:
        cwd = os.path.dirname(os.path.abspath(__file__))
        fn = os.path.join(cwd, 'saga/VERSION')
        version = open(fn).read().strip()
    except IOError:
        from subprocess import Popen, PIPE, STDOUT
        import re

        VERSION_MATCH = re.compile(r'\d+\.\d+\.\d+(\w|-)*')

        try:
            p = Popen(['git', 'describe', '--tags', '--always'],
                      stdout=PIPE, stderr=STDOUT)
            out = p.communicate()[0]

            if (not p.returncode) and out:
                v = VERSION_MATCH.search(out)
                if v:
                    version = v.group()
        except OSError:
            pass

    return version

#-----------------------------------------------------------------------------
# check python version. we need > 2.5, <3.x
if  sys.hexversion < 0x02050000 or sys.hexversion >= 0x03000000:
    raise RuntimeError("SAGA requires Python 2.x (2.5 or higher)")

#-----------------------------------------------------------------------------
# 
class our_install_data(install_data):

    def finalize_options(self): 
        self.set_undefined_options('install',
                                   ('install_lib', 'install_dir'))
        install_data.finalize_options(self)

    def run(self):
        install_data.run(self)
        # ensure there's a saga/VERSION file
        fn = os.path.join(self.install_dir, 'saga', 'VERSION')
        open(fn, 'w').write(update_version())
        self.outfiles.append(fn)

#-----------------------------------------------------------------------------
# 
class our_sdist(sdist):

    def make_release_tree(self, base_dir, files):
        sdist.make_release_tree(self, base_dir, files)

        fn = os.path.join(base_dir, 'saga', 'VERSION')
        open(fn, 'w').write(update_version())


class our_test(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import sys
        import subprocess
        testdir = "%s/tests/" % os.path.dirname(os.path.realpath(__file__))
        errno = subprocess.call([sys.executable, '%s/run_tests.py' % testdir,
                                '--config=%s/configs/basetests.cfg' % testdir])
        raise SystemExit(errno)


setup_args = {
    'name': "saga-python",
    'version': update_version(),
    'description': "A light-weight access layer for distributed computing infrastructure",
    'long_description': "An implementation of the OGF GFD.90 SAGA standard in Python",
    'author': "Ole Weidner, et al.",
    'author_email': "ole.weidner@rutgers.edu",
    'maintainer': "Ole Weidner",
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
        "saga.namespace",
        "saga.filesystem",
        "saga.replica",
        "saga.resource",
        "saga.advert",
        "saga.adaptors",
        "saga.adaptors.cpi",
        "saga.adaptors.cpi.job",
        "saga.adaptors.cpi.namespace",
        "saga.adaptors.cpi.filesystem",
        "saga.adaptors.cpi.replica",
        "saga.adaptors.cpi.resource",
        "saga.adaptors.cpi.advert",
        "saga.adaptors.context",
        "saga.adaptors.local",
        "saga.adaptors.shell",
        "saga.adaptors.sge",
        "saga.adaptors.pbs",
        "saga.adaptors.lsf",
        "saga.adaptors.condor",
        "saga.adaptors.slurm",
        "saga.adaptors.redis",
        "saga.adaptors.irods",
        "saga.adaptors.aws",
        "saga.adaptors.http",
        "saga.engine",
        "saga.utils",
        "saga.utils.job",
    ],
    'package_data': {'': ['*.sh']},
    'zip_safe': False,
    'scripts': [],
    # mention data_files, even if empty, so install_data is called and
    # VERSION gets copied
    'data_files': [("saga", [])],
    'cmdclass': {
        'install_data': our_install_data,
        'sdist': our_sdist,
        'test': our_test
    },
    'install_requires': ['setuptools', 'colorama', 'apache-libcloud', 'radical.utils'],
    'tests_require': ['setuptools', 'nose']
}

setup(**setup_args)
