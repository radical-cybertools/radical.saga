
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


import os
import time
import signal
import saga
import saga.utils.pty_shell   as sups
import saga.utils.test_config as sutc

import radical.utils.testing as rut


# ------------------------------------------------------------------------------
#
def test_ptyshell_ok () :
    """ Test pty_shell which runs command successfully """
    conf  = rut.get_test_config ()
    shell = sups.PTYShell (saga.Url(conf.job_service_url), conf.session)

    txt = "______1______2_____3_____"
    ret, out, _ = shell.run_sync ("printf \"%s\"" % txt)
    assert (ret == 0)    , "%s"       % (repr(ret))
    assert (out == txt)  , "%s == %s" % (repr(out), repr(txt))

    assert (shell.alive ())
    shell.finalize (True)
    assert (not shell.alive ())


# ------------------------------------------------------------------------------
#
def test_ptyshell_nok () :
    """ Test pty_shell which runs command unsuccessfully """
    conf  = rut.get_test_config ()
    shell = sups.PTYShell (saga.Url(conf.job_service_url), conf.session)

    txt = "______1______2_____3_____"
    ret, out, _ = shell.run_sync ("printf \"%s\" ; false" % txt)
    assert (ret == 1)    , "%s"       % (repr(ret))
    assert (out == txt)  , "%s == %s" % (repr(out), repr(txt))

    assert (shell.alive ())
    shell.finalize (True)
    assert (not shell.alive ())


# ------------------------------------------------------------------------------
#
def test_ptyshell_async () :
    """ Test pty_shell which runs command successfully """
    conf  = rut.get_test_config ()
    shell = sups.PTYShell (saga.Url(conf.job_service_url), conf.session)

    txt = "______1______2_____3_____\n"
    shell.run_async ("cat <<EOT")

    shell.send (txt)
    shell.send ('EOT\n')

    ret, out = shell.find_prompt ()
 
    assert (ret == 0)   , "%s"       % (repr(ret))
    assert (out == txt) , "%s == %s" % (repr(out), repr(txt))
 
    assert (shell.alive ())
    shell.finalize (True)
    assert (not shell.alive ())


# ------------------------------------------------------------------------------
#
def test_ptyshell_prompt () :
    """ Test pty_shell with prompt change """
    conf  = rut.get_test_config ()
    shell = sups.PTYShell (saga.Url(conf.job_service_url), conf.session)

    txt = "______1______2_____3_____"
    ret, out, _ = shell.run_sync ("printf \"%s\"" % txt)
    assert (ret == 0)    , "%s"       % (repr(ret))
    assert (out == txt)  , "%s == %s" % (repr(out), repr(txt))

    shell.run_sync ('export PS1="HALLO-(\\$?)-PROMPT>"', 
                     new_prompt='HALLO-\((\d)\)-PROMPT>')

    txt = "______1______2_____3_____"
    ret, out, _ = shell.run_sync ("printf \"%s\"" % txt)
    assert (ret == 0)    , "%s"       % (repr(ret))
    assert (out == txt)  , "%s == %s" % (repr(out), repr(txt))

    assert (shell.alive ())
    shell.finalize (True)
    assert (not shell.alive ())


# ------------------------------------------------------------------------------
#
def test_ptyshell_file_stage () :
    """ Test pty_shell file staging """
    conf  = rut.get_test_config ()
    shell = sups.PTYShell (saga.Url(conf.job_service_url), conf.session)

    txt = "______1______2_____3_____"
    shell.write_to_remote   (txt, "/tmp/saga-test-staging")
    out = shell.read_from_remote ("/tmp/saga-test-staging")

    assert (txt == out)  , "%s == %s" % (repr(out), repr(txt))

    ret, out, _ = shell.run_sync ("rm /tmp/saga-test-staging")
    assert (ret == 0)    , "%s"       % (repr(ret))
    assert (out == "")   , "%s == ''" % (repr(out))


# ------------------------------------------------------------------------------


