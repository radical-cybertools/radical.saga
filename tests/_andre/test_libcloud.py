

import os
from   pprint import pprint

import libcloud.compute.types      as lcct
import libcloud.compute.providers  as lccp

ec2_id  = os.environ['EC2_ID']
ec2_key = os.environ['EC2_KEY']

Driver = lccp.get_driver (lcct.Provider.EC2)
conn   = Driver (ec2_id, ec2_key)
pprint (conn)

nodes  = conn.list_nodes ()
pprint (nodes)

for node in nodes :
    pprint (node.__dict__)
    print node.destroy ()


# images = conn.list_images()
# pprint (images[0])
# pprint (images[-1])
# 
# img = None
# for image in images :
#     if  image.id.startswith ('ami-') :
#         print image
#         img = image
# 
# sizes = conn.list_sizes()
# pprint (sizes[0])
# pprint (sizes[-1])

# node = conn.create_node (name='libcloud_test', image=img, size=sizes[0])
# pprint (node)

