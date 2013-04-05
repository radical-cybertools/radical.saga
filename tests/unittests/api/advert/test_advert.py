
import saga
import saga.utils.test_config as sutc

check = False

# ------------------------------------------------------------------------------
#

def cb (obj, key, val) :

    global check
    if key == 'foo' and val == 'baz' :
        check = True
    else :
        check = False

    return True
    

# ------------------------------------------------------------------------------
#
def test_advert_callback () :

    try :
        global check

        tc = sutc.TestConfig()
        
        d_1 = saga.advert.Directory (tc.advert_url + '/tmp/test1/test1/',
                                     saga.advert.CREATE | saga.advert.CREATE_PARENTS)

        d_1.set_attribute ('foo', 'bar')
        d_1.add_callback  ('foo',  cb  )
        d_1.set_attribute ('foo', 'baz')

        assert check # check if callback got invoked
        check = False

        
    except saga.NotImplemented as ni:
            assert tc.notimpl_warn_only, "%s " % ni
            if tc.notimpl_warn_only:
                print "%s " % ni
    except saga.SagaException as se:
        assert False, "Unexpected exception: %s" % se
    


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

test_advert_callback ()

