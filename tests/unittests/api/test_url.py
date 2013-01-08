
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2011-2012, Ole Christian Weidner"
__license__   = "MIT"


from saga.url import *

def test_wrong_type():
    """ Test that the right execption is thrown if Url is not initialized
        properly. 
    """
    try:
        u = Url({'bad':'idea'})
        assert False
    except BadParameter, ex:
        assert True

###########################################################################
#
def test_url_compatibility():

    u1 = Url("ssh://user:pwd@hostname.domain:9999/path")

    if u1.scheme != "ssh":
        self.fail("unexpected value for scheme")

    if u1.username != "user":
        self.fail("unexpected value for username")

    if u1.password != "pwd":
        self.fail("unexpected value for password")

    if u1.host != "hostname.domain":
        self.fail("unexpected value for host")

    if u1.port != int(9999):
        self.fail("unexpected value for port")

###########################################################################
#
def test_url_scheme_issue():

    u1 = Url("unknownscheme://user:pwd@hostname.domain:9999/path")
    if u1.scheme != "unknownscheme":
        self.fail("unexpected value for scheme")

    if u1.username != "user":
        self.fail("unexpected value for username")

    if u1.password != "pwd":
        self.fail("unexpected value for password")

    if u1.host != "hostname.domain":
        self.fail("unexpected value for host")

    if u1.port != int(9999):
        self.fail("unexpected value for port")

###########################################################################
#
def test_url_issue_49(): 

    url = Url ("scheme://pass:user@host:123/dir/file?query#fragment")
    url.set_host ('remote.host.net')
    url.set_scheme ('sftp') 
    url.set_path ('/tmp/data')
    
    if str(url) != "sftp://pass:user@remote.host.net:123/tmp/data":
        self.fail("unexpected url")

###########################################################################
#
def test_url_issue_61(): 

    url = Url ("advert://localhost/?dbtype=sqlite3")
    
    if url.query != "dbtype=sqlite3":
        self.fail("unexpected url")

###########################################################################
#
def test_url_properties():

   url = Url("")
   if str(url) != "": 
       self.fail("unexpected url")

   url.scheme = "scheme"
   if str(url) != "scheme://":
       self.fail("unexpected url")
   if url.get_scheme() != "scheme":
       self.fail("unexpected scheme")       
   url.set_scheme("tscheme")
   if url.get_scheme() != "tscheme":
       self.fail("unexpected scheme")
   url.scheme = "scheme"

   url.host = "host"
   if str(url) != "scheme://host": 
       self.fail("unexpected url")
   if url.get_host() != "host":
       self.fail("unexpected host")       
   url.set_host("thost")
   if url.get_host() != "thost":
       self.fail("unexpected host")
   url.host = "host"

   url.port = 42
   if str(url) != "scheme://host:42":
       self.fail("unexpected url")
   if url.get_port() != 42:
       self.fail("unexpected port")       
   url.set_port(43)
   if url.get_port() != 43:
       self.fail("unexpected port")
   url.port = 42

   url.username = "username"
   if str(url) != "scheme://username@host:42": 
       self.fail("unexpected url")
   if url.get_username() != "username":
       self.fail("unexpected username")       
   url.set_username("tusername")
   if url.get_username() != "tusername":
       self.fail("unexpected username")
   url.username = "username"

   url.password = "password"
   if str(url) != "scheme://username:password@host:42":
       self.fail("unexpected url")
   if url.get_password() != "password":
       self.fail("unexpected password")       
   url.set_password("tpassword")
   if url.get_password() != "tpassword":
       self.fail("unexpected passsword")
   url.password = "password"

   url.path = "/path/"
   if str(url) != "scheme://username:password@host:42/path/":
       self.fail("unexpected url")
   if url.get_path() != "/path":
       self.fail("unexpected path")       
   url.set_path("tpath")
   if url.get_path() != "/tpath":
       self.fail("unexpected path")
   url.path = "/path/"


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

