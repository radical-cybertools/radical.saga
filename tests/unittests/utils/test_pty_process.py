
import os
import time
import signal
from   saga.utils.pty_process import PTYProcess

# ------------------------------------------------------------------------------
#
def test_ptyprocess_ok () :
    """ Test pty_process which finishes successfully """
    pty = PTYProcess ("/bin/true")
    pty.wait ()
    assert pty.exit_code == 0  # true returns 0

# ------------------------------------------------------------------------------
#
def test_ptyprocess_nok () :
    """ Test pty_process which finishes unsuccessfully """
    pty = PTYProcess ("/bin/false")
    pty.wait ()
    assert pty.exit_code != 0  # false returns 1

# ------------------------------------------------------------------------------
#
def test_ptyprocess_term () :
    """ Test pty_process which gets terminated """
    pty = PTYProcess ("/bin/sleep 100")
    os.kill (pty.child, signal.SIGTERM)
    time.sleep (1)
    assert (not pty.alive ())                  # killed it - it better stays dead!
    assert (pty.exit_signal == signal.SIGTERM) # killed it with SIGTERM

# ------------------------------------------------------------------------------
#
def test_ptyprocess_kill () :
    """ Test pty_process which gets killed """
    pty = PTYProcess ("/bin/sleep 100")
    os.kill (pty.child, signal.SIGKILL)
    time.sleep (1)
    assert (not pty.alive ())                  # killed it - it better stays dead!
    assert (pty.exit_signal == signal.SIGKILL) # killed it with SIGKILL

# ------------------------------------------------------------------------------
#
def test_ptyprocess_suspend_resume () :
    """ Test pty_process which gets suspended/resumed """
    pty = PTYProcess ("/bin/sleep 100")
    os.kill (pty.child, signal.SIGSTOP)
    os.kill (pty.child, signal.SIGCONT)
    assert (pty.alive ())                      # don't play dead!

# ------------------------------------------------------------------------------
#
def test_ptyprocess_stdout () :
    """ Test pty_process printing stdout messages"""
    txt = 'hello world\n'
    pty = PTYProcess ("printf \"%s\"" % txt)
    pty.wait ()
    out = pty.read ()
    assert (str(txt.strip ()) == str(out.strip ()))

# ------------------------------------------------------------------------------
#
def test_ptyprocess_stderr () :
    """ Test pty_process printing stderr messages"""
    txt = 'hello world\n'
    pty = PTYProcess ("sh -c 'echo \"%s\" 1>&2'" % txt)
    out = pty.read ()
    assert (str(txt.strip ()) == str(out.strip ()))

# ------------------------------------------------------------------------------
#
def test_ptyprocess_write () :
    """ Test pty_process reading stdin, printing stdout messages"""
    txt = "1\n2\n3\n"
    pty = PTYProcess ("cat")
    pty.write (txt)
    out = pty.read ()
    assert (txt.strip () == out.strip())

# ------------------------------------------------------------------------------
#
def test_ptyprocess_find () :
    """ Test pty_process selecting stdout messages"""
    txt = "1\n2\n3\n"
    pty = PTYProcess ("printf \"%s\"" % txt)
    out = pty.find ('2', '3')
    assert (out == (0, '1\n2'))

