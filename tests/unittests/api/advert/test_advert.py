#!/usr/bin/env python

import radical.utils as ru
import radical.saga  as rs

check = False


# ------------------------------------------------------------------------------
#
def config():

    ru.set_test_config(ns='radical.saga')
    ru.add_test_config(ns='radical.saga', cfg_name='advert_redis_local')

    return ru.get_test_config()


# ------------------------------------------------------------------------------
#
def cb (obj, key, val) :

    global check
    if key == 'foo' and val == 'baz':
        check = True
    else:
        check = False

    return True


# ------------------------------------------------------------------------------
#
def test_advert_callback():

    try:
        global check

        tc  = config()
        d_1 = rs.advert.Directory (tc.advert_url + '/tmp/test1/test1/',
                                   rs.advert.CREATE | rs.advert.CREATE_PARENTS)

        d_1.set_attribute ('foo', 'bar')
        d_1.add_callback  ('foo',  cb  )
        d_1.set_attribute ('foo', 'baz')

        assert check  # check if callback got invoked
        check = False

    except rs.NotImplemented as ni:
        assert bool(tc.notimpl_warn_only), "%s " % ni
        if tc.notimpl_warn_only:
            print("%s " % ni)

    except rs.SagaException as se:
        assert False, "Unexpected exception: %s" % se


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_advert_callback ()


# ------------------------------------------------------------------------------

