
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import saga.exceptions as se
from   saga.url import Url

import radical.utils as ru


def test_wrong_type():
    """ Test that the right execption is thrown if Url is not initialized
        properly. 
    """
    try:
        u = Url({'bad':'idea'})
        assert False
    except se.BadParameter, ex:
        assert True

# -------------------------------------------------------------------------
#
def test_url_compatibility():

    u1 = Url("ssh://user:pwd@hostname.domain:9999/path")

    assert u1.scheme   == "ssh",             "unexpected value for scheme" 
    assert u1.username == "user",            "unexpected value for username" 
    assert u1.password == "pwd",             "unexpected value for password" 
    assert u1.host     == "hostname.domain", "unexpected value for host" 
    assert u1.port     == int(9999),         "unexpected value for port" 


###########################################################################
#
def test_url_scheme_issue():

    u1 = Url("unknownscheme://user:pwd@hostname.domain:9999/path")

    assert u1.scheme   == "unknownscheme",   "unexpected value for scheme" 
    assert u1.username == "user",            "unexpected value for username" 
    assert u1.password == "pwd",             "unexpected value for password" 
    assert u1.host     == "hostname.domain", "unexpected value for host" 
    assert u1.port     == int(9999),         "unexpected value for port" 

###########################################################################
#
def test_url_issue_49(): 

    url = Url ("scheme://pass:user@host:123/dir/file?query#fragment")
    url.set_host   ('remote.host.net')
    url.set_scheme ('sftp') 
    url.set_path   ('/tmp/data')
    
    assert str(url) == "sftp://pass:user@remote.host.net:123/tmp/data", "unexpected url" 

###########################################################################
#
def test_url_issue_61(): 

    url = Url ("advert://localhost/?dbtype=sqlite3")
    assert url.query == "dbtype=sqlite3", "unexpected url" 

###########################################################################
#
def test_url_properties():

   url = Url("")
   assert str(url)           == "",                 "unexpected url" 

   url.scheme = "scheme"
   assert str(url)           == "scheme://",        "unexpected url" 
   assert url.get_scheme()   == "scheme",           "unexpected scheme"

   url.set_scheme("tscheme")
   assert url.get_scheme()   == "tscheme",          "unexpected scheme" 

   url.scheme = "scheme"
   url.host   = "host"
   assert str(url)           == "scheme://host":,   "unexpected url" 
   assert url.get_host()     == "host",             "unexpected host")      
   
   url.set_host("thost")
   assert url.get_host()     == "thost",            "unexpected host" 
   
   url.host = "host"
   url.port = 42
   assert str(url)           == "scheme://host:42", "unexpected url" 
   assert url.get_port()     == 42,                 "unexpected port"

   url.set_port(43)
   assert url.get_port()     == 43,                 "unexpected port" 

   url.port     = 42
   url.username = "username"
   assert str(url)           == "scheme://username@host:42":, "unexpected url" 
   assert url.get_username() == "username",         "unexpected username"
   
   url.set_username("tusername")
   assert url.get_username() == "tusername",        "unexpected username" 
   
   url.username = "username"
   url.password = "password"
   assert str(url)           == "scheme://username:password@host:42", "unexpected url" 
   assert url.get_password() == "password",         "unexpected password"
   
   url.set_password("tpassword")
   assert url.get_password() == "tpassword",        "unexpected passsword" 
   
   url.password = "password"
   url.path     = "/path/"
   assert str(url)           == "scheme://username:password@host:42/path/", "unexpected url" 
   assert url.get_path()     == "/path",            "unexpected path"

   url.set_path("tpath")
   assert url.get_path()     == "/tpath",           "unexpected path" 
   



