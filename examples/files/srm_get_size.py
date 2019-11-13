#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

'''This examples shows how to copy a file to a remote
   host via SRM using a custom security context.

   If something doesn't work as expected, try to set
   SAGA_VERBOSE=3 in your environment before you run the
   script in order to get some debug output.

   If you think you have encountered a defect, please
   report it at: https://github.com/radical-cybertools/radical.saga/issues
'''

__author__    = "Mark Santcroos"
__copyright__ = "Copyright 2013, Mark Santcroos"
__license__   = "MIT"


import sys

import radical.saga as rs


def main():

    try:
        myfile = rs.filesystem.File('srm://tbn18.nikhef.nl/dpm/nikhef.nl/home/vlemed/mark/radical.saga/input.txt')
      # nofile = rs.filesystem.File('srm://tbn18.nikhef.nl/dpm/nikhef.nl/home/vlemed/mark/radical.saga/input.txt-non-exist')

      # mydir  = rs.filesystem.Directory('srm://tbn18.nikhef.nl/dpm/nikhef.nl/home/vlemed/mark/radical.saga')
      # nodir  = rs.filesystem.File('srm://tbn18.nikhef.nl/dpm/nikhef.nl/home/vlemed/mark/radical.saga-non-exists/input.txt-non-exist')

        print(myfile.get_size_self())
      # print myfile_non_exist.get_size_self()
      # print mydir.get_size()
      # print mydir_non_exist.get_size()

    except rs.SagaException as ex:
        print("An error occured during file operation: %s" % (str(ex)))
        sys.exit(-1)


if __name__ == "__main__":
    main()

