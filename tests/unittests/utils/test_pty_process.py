
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


import os
import time
import signal
import saga.utils.pty_process as supp

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_ptyprocess_ok () :
    """ Test pty_process which finishes successfully """
    pty = supp.PTYProcess ("true")
    pty.wait ()
    assert pty.exit_code == 0


# ------------------------------------------------------------------------------
#
def test_ptyprocess_nok () :
    """ Test pty_process which finishes unsuccessfully """
    pty = supp.PTYProcess ("false")
    pty.wait ()
    assert pty.exit_code != 0


# ------------------------------------------------------------------------------
#
def test_ptyprocess_term () :
    """ Test pty_process which gets terminated """
    pty = supp.PTYProcess ("cat")
    os.kill (pty.child, signal.SIGTERM)
    time.sleep (0.1)
    assert (not pty.alive ())
    assert (pty.exit_signal == signal.SIGTERM), "'%s' == '%s'" % \
           (pty.exit_signal ,  signal.SIGTERM)

# ------------------------------------------------------------------------------
#
def test_ptyprocess_kill () :
    """ Test pty_process which gets killed """
    pty = supp.PTYProcess ("cat")
    os.kill (pty.child, signal.SIGKILL)
    time.sleep (0.1)
    assert (not pty.alive ())
    assert (pty.exit_signal == signal.SIGKILL), "'%s' == '%s'" % \
           (pty.exit_signal ,  signal.SIGKILL)


# ------------------------------------------------------------------------------
#
def test_ptyprocess_suspend_resume () :
    """ Test pty_process which gets suspended/resumed """
    pty = supp.PTYProcess ("cat")
    os.kill (pty.child, signal.SIGSTOP)
    os.kill (pty.child, signal.SIGCONT)
    time.sleep (0.1)
    assert (pty.alive ())


# ------------------------------------------------------------------------------
#
def test_ptyprocess_stdout () :
    """ Test pty_process printing stdout messages"""
    txt = "______1______2_____3_____\n"
    pty = supp.PTYProcess ("printf \"%s\"" % txt)
    out = pty.read (size=len(txt), timeout=1.0)
    pty.wait ()
    assert (str(txt) == str(out)), "'%s' == '%s'" % \
           (str(txt) ,  str(out))


# ------------------------------------------------------------------------------
#
def test_ptyprocess_stderr () :
    """ Test pty_process printing stderr messages"""
    txt = "______1______2_____3_____\n"
    pty = supp.PTYProcess ("sh -c 'printf \"%s\" 1>&2'" % txt)
    out = pty.read (size=len(txt), timeout=1.0)
  # print "--%s--%s--\n" % ( len(txt), txt)
  # print "--%s--%s--\n" % ( len(out), out)
    assert (str(txt) == str(out)), "'%s' == '%s'" % \
           (str(txt) ,  str(out))


# ------------------------------------------------------------------------------
#
def test_ptyprocess_write () :
    """ Test pty_process reading stdin, printing stdout messages"""
    # cat is line buffered, thus need \n
    txt = "______1______2_____3_____\n"
    pty = supp.PTYProcess ("cat")
    pty.write (txt)
    out = pty.read (size=len(txt), timeout=1.0)
  # print "--%s--%s--\n" % ( len(txt), txt)
  # print "--%s--%s--\n" % ( len(out), out)
    assert (txt == out), "'%s' == '%s'" % \
           (txt ,  out)



# ------------------------------------------------------------------------------
#
def test_ptyprocess_find () :
    """ Test pty_process selecting stdout messages"""
    txt = "______1_____2______3_____"
    pty = supp.PTYProcess ("printf \"%s\"" % txt)
    out = pty.find ('2', '3')
  # print out
    assert (out == (0, '______1_____2')), "'%s' == '%s'" % \
           (out ,  (0, '______1_____2'))


# ------------------------------------------------------------------------------
#
def test_ptyprocess_restart () :
    """ Test pty_process restart"""
    pty = supp.PTYProcess ("cat")
    assert (pty.alive ())

    pty.finalize ()
    assert (not pty.alive ())

    pty.initialize ()
    assert (pty.alive ())

    pty.finalize ()
    assert (not pty.alive ())

