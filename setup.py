#!/usr/bin/env python3

__author__    = 'RADICAL-Cybertools Team'
__email__     = 'info@radical-cybertools.org'
__copyright__ = 'Copyright 2013-23, The RADICAL-Cybertools Team'
__license__   = 'MIT'


''' Setup script, only usable via pip. '''

import os

import subprocess as sp

from glob       import glob
from setuptools import setup, Command, find_namespace_packages


# ------------------------------------------------------------------------------
#
base     = 'saga'
name     = 'radical.%s'      % base
mod_root = 'src/radical/%s/' % base

scripts  = list(glob('bin/*'))
root     = os.path.dirname(__file__) or '.'
readme   = open("%s/README.md" % root, encoding='utf-8').read()
descr    = "A light-weight access layer for distributed compute infrastructure"
keywords = ['radical', 'cybertools', 'utilities', 'saga', 'job', 'compute']

share    = 'share/%s' % name
data     = [('%s/examples'      % share, glob('examples/*.{py,cfg,json,sh}')),
]


# ------------------------------------------------------------------------------
#
def sh_callout(cmd):
    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
    stdout, stderr = p.communicate()
    ret            = p.returncode
    return stdout, stderr, ret


# ------------------------------------------------------------------------------
#
def get_version(_mod_root):
    '''
    a VERSION file containes the version strings is created in mod_root,
    during installation.  That file is used at runtime to get the version
    information.
    '''

    _out = None
    _err = None
    _ret = None
    try:
        _version_path   = '%s/%s/VERSION' % (root, _mod_root)
        _version_base   = None
        _version_short  = None
        _version_branch = None
        _version_tag    = None
        _version_detail = None

        # get `version_base` from distribution's 'VERSION' file
        with open('%s/VERSION' % root, 'r', encoding='utf-8') as fin:
            _version_base = fin.readline().strip()

        _, _, ret = sh_callout('cd %s && git rev-parse --git-dir && which git'
                               % root)
        _in_git = (ret == 0)

        if not _in_git:

            with open(_version_path, 'w', encoding='utf-8') as fout:
                fout.write(_version_base + '\n')

        else:

            # get details from git
            _out, _err, _ret = sh_callout('cd %s && git describe --tags --always' % root)
            assert _ret == 0, 'git describe failed'
            _out = _out.decode()
            _out = _out.strip()

            _version_tag = _out

            _out, _err, _ret = sh_callout('cd %s && git branch --show-current' % root)
            assert _ret == 0, 'git branch failed'

            _out = _out.decode()
            _out = _out.strip()

            _version_branch = _out or 'detached'
            _version_branch = _version_branch.replace('detached from ', '~')

            _version_short = _version_tag.split('-')[0]
            _version_short = _version_short[1:]  # strip the 'v'

            if _version_tag:
                _version_detail = '%s-%s@%s' % (_version_base, _version_tag,
                                                _version_branch)
            else:
                _version_detail = '%s@%s' % (_version_base, _version_branch)

            with open(_version_path, 'w', encoding='utf-8') as fout:
                fout.write(_version_short  + '\n')
                fout.write(_version_base   + '\n')
                fout.write(_version_branch + '\n')
                fout.write(_version_tag    + '\n')
                fout.write(_version_detail + '\n')

        return _version_base, _version_path

    except Exception as e:
        _msg = 'Could not extract/set version: %s' % e
        if _ret:
            _msg += '\n' + _out + '\n\n' + _err
        raise RuntimeError(_msg) from e


# ------------------------------------------------------------------------------
# get version info -- this will create VERSION and srcroot/VERSION
version, version_path = get_version(mod_root)


# ------------------------------------------------------------------------------
#
class RunTwine(Command):
    user_options = []
    def initialize_options(self): pass
    def finalize_options(self):   pass
    def run(self):
        _, _, _ret = sh_callout('python3 setup.py sdist upload -r pypi')
        raise SystemExit(_ret)


# ------------------------------------------------------------------------------
#
with open('%s/requirements.txt' % root, encoding='utf-8') as freq:
    requirements = freq.readlines()


# ------------------------------------------------------------------------------
#
setup_args = {
    'name'               : name,
    'version'            : version,
    'description'        : descr,
    'long_description'   : readme,
    'long_description_content_type' : 'text/markdown',
    'author'             : 'RADICAL Group at Rutgers University',
    'author_email'       : 'radical@rutgers.edu',
    'maintainer'         : 'The RADICAL Group',
    'maintainer_email'   : 'radical@rutgers.edu',
    'url'                : 'http://radical-cybertools.github.io/%s/' % name,
    'project_urls'       : {
        'Documentation': 'https://radical%s.readthedocs.io/en/latest/' % base,
        'Source'       : 'https://github.com/radical-cybertools/%s/'   % name,
        'Issues' : 'https://github.com/radical-cybertools/%s/issues'   % name,
    },
    'license'            : 'MIT',
    'keywords'           : keywords,
    'python_requires'    : '>=3.7',
    'classifiers'        : [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Topic :: Utilities',
        'Topic :: System :: Distributed Computing',
        'Topic :: Scientific/Engineering',
        'Operating System :: POSIX',
        'Operating System :: Unix'
    ],
    'packages'           : find_namespace_packages('src', include=['radical.*']),
    'package_dir'        : {'': 'src'},
    'scripts'            : scripts,
    'package_data'       : {'': ['*.txt', '*.sh', '*.json', '*.gz', '*.c',
                                 '*.md', 'VERSION']},
    'install_requires'   : requirements,
    'zip_safe'           : False,
    'data_files'         : data,
    'cmdclass'           : {'upload': RunTwine},
}


# ------------------------------------------------------------------------------
#
setup(**setup_args)


# ------------------------------------------------------------------------------
# clean temporary files from source tree
os.system('rm -vrf src/%s.egg-info' % name)
os.system('rm -vf  %s'              % version_path)


# ------------------------------------------------------------------------------

