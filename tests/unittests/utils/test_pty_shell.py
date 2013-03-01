
import os
import time
import signal
import saga
import saga.utils.pty_shell   as sups
import saga.utils.test_config as sutc


## # ------------------------------------------------------------------------------
## #
## def test_ptyshell_ok () :
##     """ Test pty_shell which runs command successfully """
##     conf  = sutc.TestConfig()
##     shell = sups.PTYShell (saga.Url(conf.js_url), conf.session.contexts)
## 
##     txt = "______1______2_____3_____"
##     ret, out, _ = shell.run_sync ("printf \"%s\"" % txt)
##     assert (ret == 0)
##     assert (out == txt)
## 
##     assert (shell.alive ())
##     shell.run_async ("exit")
##     time.sleep (1)
##     assert (not shell.alive ())
## 
## 
## # ------------------------------------------------------------------------------
## #
## def test_ptyshell_nok () :
##     """ Test pty_shell which runs command unsuccessfully """
##     conf  = sutc.TestConfig()
##     shell = sups.PTYShell (saga.Url(conf.js_url), conf.session.contexts)
## 
##     txt = "______1______2_____3_____"
##     ret, out, _ = shell.run_sync ("printf \"%s\" ; false" % txt)
##     assert (ret == 1)
##     assert (out == txt)
## 
##     assert (shell.alive ())
##     shell.run_async ("exit")
##     time.sleep (1)
##     assert (not shell.alive ())
## 
## 
## # ------------------------------------------------------------------------------
## #
## def test_ptyshell_async () :
##     """ Test pty_shell which runs command successfully """
##     conf  = sutc.TestConfig()
##     shell = sups.PTYShell (saga.Url(conf.js_url), conf.session.contexts)
## 
##     txt = "______1______2_____3_____\n"
##     shell.run_async ("cat <<EOT")
## 
##     shell.send (txt)
##     shell.send ('EOT\n')
## 
##     ret, out = shell.find_prompt ()
##  
##     assert (ret == 0)
##     assert (out == "%s" % txt)
##  
##     assert (shell.alive ())
##     shell.run_async ("exit")
##     time.sleep (1)
##     assert (not shell.alive ())
## 
## 
## # ------------------------------------------------------------------------------
## #
## def test_ptyshell_prompt () :
##     """ Test pty_shell with prompt change """
##     conf  = sutc.TestConfig()
##     shell = sups.PTYShell (saga.Url(conf.js_url), conf.session.contexts)
## 
##     txt = "______1______2_____3_____"
##     ret, out, _ = shell.run_sync ("printf \"%s\"" % txt)
##     assert (ret == 0)
##     assert (out == txt)
## 
##     shell.run_sync ('export PS1="HALLO-(\\$?)-PROMPT>"', 
##                      new_prompt='HALLO-\((\d)\)-PROMPT>')
## 
##     txt = "______1______2_____3_____"
##     ret, out, _ = shell.run_sync ("printf \"%s\"" % txt)
##     assert (ret == 0)
##     assert (out == txt)
## 
##     assert (shell.alive ())
##     shell.run_async ("exit")
##     time.sleep (1)
##     assert (not shell.alive ())
## 

# ------------------------------------------------------------------------------
#
def test_ptyshell_file_stage () :
    """ Test pty_shell file staging """
    conf  = sutc.TestConfig()
    shell = sups.PTYShell (saga.Url(conf.js_url), conf.session)

    txt = "______1______2_____3_____\n"
    shell.write_to_file (txt, "/tmp/saga-test-staging")
    out = shell.read_from_file ("/tmp/saga-test-staging")

    assert (txt == out)

    ret, out, _ = shell.run_sync ("rm /tmp/saga-test-staging")
    assert (ret == 0)
    assert (out == "")


