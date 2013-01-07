
import time
import saga

d_1 = saga.advert.Directory ('redis://:securedis@localhost/tmp/test1/test1/')
d_1.set_attribute ('foo', 'doh')
  
