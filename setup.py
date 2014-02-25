
__author__    = "RADICAL Team"
__copyright__ = "Copyright 2013, RADICAL Research, Rutgers University"
__license__   = "MIT"


""" Setup script. Used by easy_install and pip. """

import os
import sys
import subprocess

from setuptools import setup, Command

srcroot = 'saga'
name    = 'SAGA-Python'
lname   = name.lower()

#-----------------------------------------------------------------------------
#
# versioning mechanism:
#
#   - short_version:  1.2.3                   - is used for installation
#   - long_version:   1.2.3-9-g0684b06-devel  - is used as runtime (ru.version)
#   - both are derived from the last git tag and branch information
#   - VERSION files are created on demand, with the long_version
#
# can't use radical.utils versioning detection, as radical.utils is only
# below specified as dependency :/
def get_version (paths=None):
    """
    paths:
        a VERSION file containing the long version is created in every directpry
        listed in paths.  Those VERSION files are used when they exist to get
        the version numbers, if they exist prior to calling this method.  If 
        not, we cd into the first path, try to get version numbers from git tags 
        in that location, and create the VERSION files in all dirst given in 
        paths.
    """

    try:

        if  None == paths :
            # by default, get version for myself
            pwd     = os.path.dirname (__file__)
            root    = "%s/.." % pwd
            paths = [root, pwd]

        if  not isinstance (paths, list) :
            paths = [paths]

        # if in any of the paths a VERSION file exists, we use the long version
        # in there.
        long_version  = None
        short_version = None
        branch_name   = None

        for path in paths :
            try :
                filename = "%s/VERSION" % path
                with open (filename) as f :
                    lines = [line.strip() for line in f.readlines()]

                    if len(lines) >= 1 : long_version  = lines[0]
                    if len(lines) >= 2 : short_version = lines[1]
                    if len(lines) >= 3 : branch_name   = lines[2]

                    if  long_version :
                        print 'reading  %s' % filename
                        break

            except Exception as e :
                pass

        # if we didn't find it, get it from git 
        if  not long_version :

            import subprocess as sp
            import re

            # make sure we look at the right git repo
            if  len(paths) :
                git_cd  = "cd %s ;" % paths[0]

            # attempt to get version information from git
            p   = sp.Popen ('%s'\
                            'git describe --tags --always ; ' \
                            'git branch   --contains | grep -e "^\*"' % git_cd,
                            stdout=sp.PIPE, stderr=sp.STDOUT, shell=True)
            out = p.communicate()[0]

            if  p.returncode != 0 or not out :

                # the git check failed -- its likely that we are called from
                # a tarball, so use ./VERSION instead
                out=open ("%s/VERSION" % paths[0], 'r').read().strip()


            pattern = re.compile ('(?P<long>(?P<short>[\d\.]+)\D.*)(\s+\*\s+(?P<branch>\S+))?')
            match   = pattern.search (out)

            if  match :
                long_version  = match.group ('long')
                short_version = match.group ('short')
                branch_name   = match.group ('branch')
                print 'inspecting git for version info'

            else :
                import sys
                sys.stderr.write ("Cannot determine version from git or ./VERSION\n")
                sys.exit (-1)
                

            if  branch_name :
                long_version = "%s-%s" % (long_version, branch_name)


        # make sure the version files exist for the runtime version inspection
        for path in paths :
            vpath = '%s/VERSION' % path
            print 'creating %s'  % vpath
            with open (vpath, 'w') as f :
                f.write (long_version  + "\n")
                f.write (short_version + "\n")
                f.write (branch_name   + "\n")
    
        return short_version, long_version, branch_name


    except Exception as e :
        print 'Could not extract/set version: %s' % e
        import sys
        sys.exit (-1)


#-----------------------------------------------------------------------------
# get version info -- this will create VERSION and srcroot/VERSION
root     = os.path.dirname (__file__)
src_dir = "%s/%s" % (root, srcroot)
short_version, long_version, branch = get_version ([root, src_dir])


#-----------------------------------------------------------------------------
# check python version. we need > 2.5, <3.x
if  sys.hexversion < 0x02050000 or sys.hexversion >= 0x03000000:
    raise RuntimeError("%s requires Python 2.x (2.5 or higher)" % name)


#-----------------------------------------------------------------------------
class our_test(Command):
    user_options = []
    def initialize_options (self) : pass
    def finalize_options   (self) : pass
    def run (self) :
        testdir = "%s/tests/" % os.path.dirname(os.path.realpath(__file__))
        retval  = subprocess.call([sys.executable,
                                   '%s/run_tests.py'               % testdir,
                                   '%s/configs/basetests.cfg'      % testdir])
        raise SystemExit(retval)


#-----------------------------------------------------------------------------
#
def read(*rnames):
    return open(os.path.join(os.path.dirname(__file__), *rnames)).read()


#-----------------------------------------------------------------------------
setup_args = {
    'name'             : name,
    'version'          : short_version,
    'description'      : "A light-weight access layer for distributed computing infrastructure",
    'long_description' : (read('README.md') + '\n\n' + read('CHANGES.md')),
    'author'           : "The RADICAL Group",
    'author_email'     : "ole.weidner@rutgers.edu",
    'maintainer'       : "Ole Weidner",
    'maintainer_email' : "ole.weidner@rutgers.edu",
    'url'              : "http://saga-project.github.com/saga-python/",
    'license'          : "MIT",
    'keywords'         : "radical pilot job saga",
    'classifiers'      : [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Utilities',
        'Topic :: System :: Distributed Computing',
        'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
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
        "saga.adaptors.loadl",
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
    'scripts'              : [],
    'package_data'         : {'' : ['*.sh', 'VERSION']},
    'cmdclass'             : {
        'test'             : our_test, 
    },
    'install_requires'     : ['apache-libcloud', 'radical.utils'],
    'tests_require'        : ['nose'],
    'zip_safe'             : False,
}

#-----------------------------------------------------------------------------

setup (**setup_args)

#-----------------------------------------------------------------------------

