
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


# assert (False), 'deprecated'

import radical.utils.testing  as rut
import radical.saga           as rs


# ----------------------------------------------------------------------
#
def configure_jd (cfg, jd) :

    for a in ['job_walltime_limit' ,
              'job_project'        ,
              'job_queue'          ,
              'job_total_cpu_count',
              'job_spmd_variation' ] :

        if  cfg.get (a, None) :
            jd.set_attribute (a, cfg[a])

    return jd


# ----------------------------------------------------------------------
#
def assert_exception (cfg, e) :

    ni = cfg.get ('not_implemented', 'warn')

    if  'NotImplemented' in str(e) and ni == 'warn' :
        print("WARNING: %s")
        return

    else :
        assert (False), "unexpected exception '%s'" % e
        raise e


# ----------------------------------------------------------------------
#
def silent_cancel (obj) :

    if  not isinstance (obj, list) :
        obj = [obj]

    for o in obj :
        try :
            o.cancel ()
        except Exception :
            pass


# ----------------------------------------------------------------------
#
def silent_close (obj) :

    if  not isinstance (obj, list) :
        obj = [obj]

    for o in obj :
        try :
            o.close ()
        except Exception :
            pass


# ------------------------------------------------------------------------------
#
class TestConfig (rut.TestConfig):

    # --------------------------------------------------------------------------
    #
    def __init__ (self, cfg_file):

        # initialize configuration.  We only use the 'rs.tests' category from
        # the config file.
        rut.TestConfig.__init__ (self, cfg_file, 'radical.saga.tests')

        # setup a saga session for the tests
        # don't populate session with default contexts...
        self.session = rs.Session (default=False)

        # attempt to create a context from the test config
        if  self.context_type :

            c = rs.Context (self.context_type)

            c.user_id    = self.context_user_id
            c.user_pass  = self.context_user_pass
            c.user_cert  = self.context_user_cert
            c.user_proxy = self.context_user_proxy

            # add it to the session
            self.session.add_context (c)


# ------------------------------------------------------------------------------

