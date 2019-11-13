__author__    = "Mark Santcroos"
__copyright__ = "Copyright 2015, The RADICAL Group"
__license__   = "MIT"

'''This tests verifies the proper execution of transfers
   with the Globus Online file adaptor.
'''

import sys
import os
import tempfile
import shutil
import radical.saga as saga

CLEANUP=False

#SOURCE='marksant#netbook'
SOURCE='localhost'
TARGET='xsede#stampede'

TEST_NAME="GO_DIR"

FILE_A_level_0 = 'file_A_level_0'
FILE_B_level_0 = 'file_B_level_0'
FILE_A_level_1 = 'file_A_level_1'

NON_EXISTING_FILE = 'NON_EXISTENT'

LEVEL_1 = 'dir_level_1'

COPIED_SUFFIX = "_COPY"

def touch(dir, file):
    # "touch" file
    open(os.path.join(dir, file), "a").close()

def main():

    tmp_dir = None

    try:

        tmp_dir = tempfile.mkdtemp(prefix='saga-test-', suffix='-%s' % TEST_NAME,
                                   dir=os.path.expanduser('~/tmp'))

        print('tmpdir: %s' % tmp_dir)

        ctx = saga.Context("x509")
        ctx.user_proxy = '/Users/mark/proj/myproxy/xsede.x509'

        session = saga.Session()
        session.add_context(ctx)

        source_url = saga.Url()
        source_url.schema = 'go'
        source_url.host = SOURCE
        source_url.path = tmp_dir

        target_url = saga.Url()
        target_url.schema = 'go'
        target_url.host = TARGET
        target_url.path = os.path.join('~/saga-tests/', os.path.basename(tmp_dir))

        print("Point to local Directory through GO ...")
        d = saga.filesystem.Directory(source_url)
        print("And check ...")
        assert d.is_dir() == True
        assert d.is_file() == False
        assert d.is_link() == False
        d.close()
        print("Point to remote Directory through GO ...")
        d = saga.filesystem.Directory(target_url, flags=saga.filesystem.CREATE_PARENTS)
        print("And check ...")
        assert d.is_dir() == True
        assert d.is_file() == False
        assert d.is_link() == False
        d.close()

        print("Point to local file through GO, before creation ...")
        caught = False
        try:
            saga.filesystem.File(os.path.join(str(source_url), FILE_A_level_0))
        except saga.DoesNotExist:
            caught = True
        assert caught == True

        print("Create actual file ...")
        touch(tmp_dir, FILE_A_level_0)
        print("Try again ...")
        f = saga.filesystem.File(os.path.join(str(source_url), FILE_A_level_0))
        assert f.is_file() == True
        assert f.is_dir() == False
        assert f.is_link() == False
        f.close()

        print("Copy local file to remote, using different filename ...")
        d = saga.filesystem.Directory(target_url, flags=saga.filesystem.CREATE_PARENTS)
        d.copy(os.path.join(str(source_url), FILE_A_level_0), FILE_A_level_0+COPIED_SUFFIX)
        d.close()
        f = saga.filesystem.File(os.path.join(str(target_url), FILE_A_level_0+COPIED_SUFFIX))
        assert f.is_file() == True
        assert f.is_dir() == False
        assert f.is_link() == False
        f.close()

        print("Copy local file to remote, keeping filename in tact ...")
        d = saga.filesystem.Directory(target_url, flags=saga.filesystem.CREATE_PARENTS)
        d.copy(os.path.join(str(source_url), FILE_A_level_0), FILE_A_level_0)
        d.close()
        f = saga.filesystem.File(os.path.join(str(target_url), FILE_A_level_0))
        assert f.is_file() == True
        assert f.is_dir() == False
        assert f.is_link() == False
        f.close()

        print('Create file in level 1 ...')
        tree = LEVEL_1
        os.mkdir(os.path.join(tmp_dir, tree))
        touch(os.path.join(tmp_dir, tree), FILE_A_level_1)
        print("Test local file ...")
        f = saga.filesystem.File(os.path.join(str(source_url), tree, FILE_A_level_1))
        assert f.is_file() == True
        assert f.is_dir() == False
        assert f.is_link() == False
        f.close()

        print("Copy local file to remote, keeping filename in tact ...")
        d = saga.filesystem.Directory(os.path.join(str(target_url), tree), flags=saga.filesystem.CREATE_PARENTS)
        d.copy(os.path.join(str(source_url), tree, FILE_A_level_1), FILE_A_level_1)
        d.close()

        print("Test file after transfer ...")
        f = saga.filesystem.File(os.path.join(str(target_url), tree, FILE_A_level_1))
        assert f.is_file() == True
        assert f.is_dir() == False
        assert f.is_link() == False
        f.close()

        print("Copy non-existent local file to remote, keeping filename in tact ...")
        d = saga.filesystem.Directory(str(target_url), flags=saga.filesystem.CREATE_PARENTS)
        try:
            d.copy(os.path.join(str(source_url), NON_EXISTING_FILE), NON_EXISTING_FILE)
        except saga.DoesNotExist:
            caught = True
        assert caught == True

        print("Test file after (non-)transfer ...")
        caught = False
        try:
            saga.filesystem.File(os.path.join(str(target_url), NON_EXISTING_FILE))
        except saga.DoesNotExist:
            caught = True
        assert caught == True

        # destination = "go://gridftp.stampede.tacc.xsede.org/~/tmp/"
        # #destination = "go://oasis-dm.sdsc.xsede.org/~/tmp/"
        # #destination = "go://ncsa#BlueWaters/~/tmp/"
        # #destination = "go://marksant#netbook/Users/mark/tmp/go/"
        # src_filename = "my_file"
        # dst_filename = "my_file_"
        # rt_filename = "my_file__"
        #
        # # open home directory on a remote machine
        # source_dir = saga.filesystem.Directory(source)
        #
        # # copy .bash_history to /tmp/ on the local machine
        # source_dir.copy(src_filename, os.path.join(destination, dst_filename))
        #
        # # list 'm*' in local /tmp/ directory
        # dest_dir = saga.filesystem.Directory(destination)
        # for entry in dest_dir.list(pattern='%s*' % src_filename[0]):
        #     print entry
        #
        # dest_file = saga.filesystem.File(os.path.join(destination, dst_filename))
        # assert dest_file.is_file() == True
        # assert dest_file.is_link() == False
        # assert dest_file.is_dir() == False
        # print 'Size: %d' % dest_file.get_size()
        #
        # dest_file.copy(source)
        #
        # dest_file.copy(os.path.join(source+'broken', rt_filename))

        print("Before return 0")
        return 0

    except saga.SagaException as ex:
        # Catch all saga exceptions
        print("An exception occurred: (%s) %s " % (ex.type, (str(ex))))
        # Trace back the exception. That can be helpful for debugging.
        print(" \n*** Backtrace:\n %s" % ex.traceback)

        print("before return -1")
        return -1

    finally:

        print("and finally ...")

        if CLEANUP and tmp_dir:
            shutil.rmtree(tmp_dir)

if __name__ == "__main__":
    sys.exit(main())
