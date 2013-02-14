
import saga

def test_deepcopy () :

    try:
        jd1 = saga.job.Description ()
        jd1.executable = '/bin/true'
        jd2 = jd1.clone ()
        jd2.executable = '/bin/false'
        assert jd1.executable != jd2.executable 

    except saga.SagaException as se:
        assert False

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

