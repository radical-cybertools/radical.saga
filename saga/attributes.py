
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" SAGA attribute interface """

from saga.exceptions import *

################################################################################

import datetime
import datetime
import traceback
import inspect
import re
from   pprint import pprint

# FIXME: add a tagging 'Monitorable' interface, which enables callbacks.


now   = datetime.datetime.now 
never = datetime.datetime.min

################################################################################
#
# define a couple of constants for the attribute API, mostly for registering
# attributes.
#
# type enums
ANY         = 'any'        # any python type can be set
URL         = 'url'        # URL type (string + URL parser checks)
INT         = 'int'        # Integer type
FLOAT       = 'float'      # float type
STRING      = 'string'     # string, duh!
BOOL        = 'bool'       # True or False or Maybe
ENUM        = 'enum'       # value is any one of a list of candidates
TIME        = 'time'       # seconds since epoch, or any py time thing
                           # which can be converted into such
                           # FIXME: conversion not implemented

# mode enums
WRITEABLE   = 'writeable'  # the consumer of the interface can change
                           # the attrib value
READONLY    = 'readonly'   # the consumer of the interface can not
                           # change the attrib value.  The
                           # implementation can still change it.
FINAL       = 'final'      # neither consumer nor implementation can
                           # change the value anymore
ALIAS       = 'alias'      # variable is deprecated, and alias'ed to
                           # a different variable.

# attrib extensions
EXTENDED    = 'extended'   # attribute added as extension
PRIVATE     = 'private'    # attribute added as private

# flavor enums
SCALAR      = 'scalar'     # the attribute value is a single data element
VECTOR      = 'vector'     # the attribute value is a list of data elements

################################################################################
#
# Callback (Abstract) Class
#
class Callback () :
    """
    Callback base class.

    All stateful objects of the pilot API allow to register a callback for any
    changes of its attributes, such as 'state' and 'state_detail'.  Those
    callbacks can be python call'ables, or derivates of this callback base
    class.  Instances which inherit this base class MUST implement (overload)
    the cb() method.

    The callable, or the callback's cb() method is what is invoked whenever the
    TROY implementation is notified of an change on the monitored object's
    attribute.

    The cb instance receives three parameters upon invocation:

      - key:  the watched attribute (e.g. 'state' or 'state_detail')
      - val:  the new value of the watched attribute
      - obj:  the watched object instance

    If the callback returns 'True', it will remain registered after invocation,
    to monitor the attribute for the next subsequent state change.  On returning
    'False' (or nothing), the callback will not be called again.

    To register a callback on a object instance, use::

      class MyCallback (troy.pilot.Callback) :

        def __init__ (self, msg) :
          self._msg = msg

        def cb (self, obj, key, val) :
          print " %s\\n %s (%s) : %s"  %  self._msg, obj, key, val

        def main () :

          cpd = troy.pilot.compute_pilot_description ()
          cps = troy.pilot.compute_pilot_service ()
          cp  = cps.submit_pilot (cpd)

          mcb = MyCallback ("Hello Pilot, how is your state?")

          cp.add_callback ('state', mcb)

    See documentation of the L{Attributes} interface for further details and
    examples.
    """

    def __init__ (self) :
        """ The callback constructor simply raises an IncorrectState exception,
            to signal that the application needs to inherit the callback class
            in a custom class in order to use notifications.
        """
        # raise IncorrectState ("Callback class must be inherited before use!")
        pass


    def __call__ (self, obj, key, val) :
        return self.cb (obj, key, val)


    def cb (self, obj, key, val) :
        """ This is the method that needs to be implemented by the application

            Keyword arguments::

                obj:  the watched object instance
                key:  the watched attribute
                val:  the new value of the watched attribute

            Return::

                keep:   bool, signals to keep (True) or remove (False) the callback
                        after invocation

            Callback invocation MAY (and in general will) happen in a separate
            thread -- so the application need to make sure that the callback
            code is thread-safe.

            The boolean return value is used to signal if the callback should
            continue to listen for events (return True) , or if it rather should
            get unregistered after this invocation (return False).
        """
        pass



################################################################################
#
#
#
class _AttributesBase (object) :
    """ 
    This class only exists to host properties -- as object itself does *not* have
    properties!  This class is not part of the public attribute API.
    """
    def __init__ (self) :
        pass


class Attributes (_AttributesBase) :
    """
    Attribute Interface Class

    The Attributes interface implements the attribute semantics of the SAGA Core
    API specification (http://ogf.org/documents/GFD.90.pdf).  Additionally, this
    implementation provides that semantics the python property interface.  Note 
    that a *single* set of attributes is internally managed, no matter what 
    interface is used for access.

    A class which uses this interface can internally specify which attributes
    can be set, and what type they have.  Also, default values can be specified,
    and the class provides a rudimentary support for converting scalar
    attributes into vector attributes and back.

    Also, the consumer of this API can register callbacks, which get triggered
    on changes to specific attribute values.

    Example use case::


        ###########################################
        class Transliterator ( pilot.Attributes ) :
            
            def __init__ (self, *args, **kwargs) :
                # setting attribs to non-extensible will cause the cal to init below to
                # complain if attributes are specified.  Default is extensible.
              # self._attributes_extensible (False)
        
                # pass args to base class init (requires 'extensible')
                super (Transliterator, self).__init__ (*args, **kwargs)
        
                # setup class attribs
                self._attributes_register   ('apple', 'Appel', URL,    SCALAR, WRITEABLE)
                self._attributes_register   ('plum',  'Pruim', STRING, SCALAR, READONLY)
        
                # setting attribs to non-extensible at *this* point will have allowed
                # custom user attribs on __init__ time (via args), but will then forbid
                # any additional custom attributes.
              # self._attributes_extensible (False)
        
        
        ###########################################
        if __name__ == "__main__":
        
            # define a callback method.  This callback can get registered for
            # attribute changes later.
        
            #################################
            def cb (key, val, obj) :
                # the callback gets information about what attribute was changed
                # on what object:
                print "called: %s - %s - %s"  %  (key, str(val), type (obj))

                # returning True will keep the callback registered for further
                # attribute changes.
                return True
            #################################
        
            # create a class instance and add a 'cherry' attribute/value on
            # creation.  
            trans = Transliterator (cherry='Kersche')
        
            # use the property interface to mess with the pre-defined
            # 'apple' attribute
            print "\\n -- apple"
            print trans.apple 
            trans.apple = 'Abbel'
            print trans.apple 
        
            # add our callback to the apple attribute, and trigger some changes.
            # Note that the callback is also triggered when the attribute's
            # value changes w/o user control, e.g. by some internal state
            # changes.
            trans.add_callback ('apple', cb)
            trans.apple = 'Apfel'
        
            # Setting an attribute final is actually an internal method, used by
            # the implementation to signal that no further changes on that
            # attribute are expected.  We use that here for demonstrating the
            # concept though.  Callback is invoked on _set_final.
            trans._attributes_set_final ('apple')
            trans.apple = 'Abbel'
            print trans.apple 
        
            # mess around with the 'plum' attribute, which was marked as
            # ReadOnly on registration time.
            print "\\n -- plum"
            print trans.plum
          # trans.plum    = 'Pflaume'  # raises readonly exception
          # trans['plum'] = 'Pflaume'  # raises readonly exception
            print trans.plum
        
            # check if the 'cherry' attribute exists, which got created on
            # instantiation time.
            print "\\n -- cherry"
            print trans.cherry
        
            # as we have 'extensible' set, we can add a attribute on the fly,
            # via either the property interface, or via the GFD.90 API of 
            # course.
            print "\\n -- peach"
            print trans.peach
            trans.peach = 'Birne'
            print trans.peach


    This example will result in::

        -- apple
        Appel
        Appel
        Abbel
        called: apple - Abbel Appel  - <class '__main__.Transliterator'>
        called: apple - Apfel - <class '__main__.Transliterator'>
        called: apple - Apfel - <class '__main__.Transliterator'>
        Apfel
        
        -- plum
        Pruim
        Pruim
        
        -- cherry
        Kersche
        
        -- peach
        Berne
        Birne


    """

    # internally used constants to distinguish API from adaptor calls
    _UP    = '_up'
    _DOWN  = '_down'

    # two regexes for converting CamelCase into under_score_casing, as static
    # class vars to avoid frequent recompilation
    _camel_case_regex_1 = re.compile('(.)([A-Z][a-z]+)')
    _camel_case_regex_2 = re.compile('([a-z0-9])([A-Z])')


    ############################################################################
    #
    #
    #
    def __init__ (self, *args, **kwargs) :
        """
        This method is not supposed to be directly called by the consumer of
        this API -- it should be called via derived object construction.

        attributes_t_init makes sure that the basic structures are in place on
        the attribute dictionary - this saves us ton of safety checks later on.
        """
        # FIXME: add the ability to initalize the attributes via a dict

        # initialize state
        d = self._attributes_t_init ()


    ############################################################################
    #
    # Internal interface tools.
    #
    # These tools are only for internal use, and should never be called from 
    # outside of this module.
    #
    # Naming: __attributes_t*
    #
    ####################################
    def _attributes_t_init (self, key=None) :
        """
        This internal function is not to be used by the consumer of this API.

        The _attributes_t_init method initializes the interface's internal data
        structures.  We always need the attribute dict, and the extensible flag.
        Everything else can be added on the fly.  The method will not overwrite
        any settings -- initialization occurs only once!

        If a key is given, the existence of this key is checked.  An exception
        is raised if the key does not exist.

        The internal data are stored as property on the _AttributesBase class.
        Storing them as property on *this* class would obviously result in
        recursion...
        """

        d = {}

        try :
            d = _AttributesBase.__getattribute__ (self, '_d')
        except :
            # need to initialize -- any exceptions in the code below should fall through
            d['_attributes']  = {}
            d['_extensible']  = True
            d['_private']     = True
            d['_camelcasing'] = False
            d['getter']       = None
            d['setter']       = None
            d['lister']       = None
            d['caller']       = None
            d['recursion']    = False

            _AttributesBase.__setattr__ (self, '_d', d)


        # check if we know about the given attribute
        if key :
            if not key in d['_attributes'] :
                raise DoesNotExist ("attribute key is invalid: %s"  %  (key))

        # all is well
        return d


    ####################################
    def _attributes_t_keycheck (self, key) :
        """
        This internal function is not to be used by the consumer of this API.

        For the given key, check if the key name is valid, and/or if it is
        aliased.  
        
        If the does not yet exist, the validity check is performed, and allows
        to limit dynamically added attribute names.
        
        if the key does exist, the alias check triggers a deprecation warning,
        and returns the aliased key for transparent operation.
        """

        # make sure interface is ready to use
        d = self._attributes_t_init ()

        # perform name validity checks if key is new
        if not key in d['_attributes'] :
            # FIXME: we actually don't have any tests, yet.  We should allow to
            # configure such via, say, _attributes_check_add (callable (key))
            pass


        # if key is known, check for aliasing
        else: 
            # check if we know about the given attribute
            if d['_attributes'][key]['mode'] == ALIAS :
                alias = d['_attributes'][key]['alias']
                print "attribute key / property name '%s' is deprecated - use '%s'"  %  (key, alias)
                key   = alias

        return key


    ####################################
    def _attributes_t_call_cb (self, key) :
        """
        This internal function is not to be used by the consumer of this API.

        It triggers the invocation of all callbacks for a given attribute.
        Callbacks returning False (or nothing at all) will be unregistered after
        their invocation.
        """

        # make sure interface is ready to use
        d = self._attributes_t_init (key)

        # avoid recursion
        if d['_attributes'][key]['recursion'] :
            return

        callbacks = d['_attributes'][key]['callbacks']

        # iterate over a copy of the callback list, so that remove does not
        # screw up the iteration
        for cb in list (callbacks) :

            cb = cb

            # got the callable - call it!
            # raise and lower recursion shield as needed
            ret = False
            try :
                d['_attributes'][key]['recursion'] = True
                ret = cb (self, key, self.__getattr__ (key))
            finally :
                d['_attributes'][key]['recursion'] = False

            # remove callbacks which return 'False', or raised and exception
            if not ret :
                callbacks.remove (cb)



    ####################################
    def _attributes_t_call_setter (self, key, val) :
        """
        This internal function is not to be used by the consumer of this API.

        It triggers the invocation of any registered setter function, usually
        after an attribute's set()
        """

        # make sure interface is ready to use.
        d = self._attributes_t_init (key)

        # avoid recursion
        if d['_attributes'][key]['recursion'] :
            return

        # no callbacks for private keys
        if key[0] == '_' and d['_private'] :
            return


        # key_setter overwrites results from all_setter
        all_setter = d['setter']
        key_setter = d['_attributes'][key]['setter']

        # Get the value via the attribute getter.  The getter will not call
        # attrib getters or callbacks, due to the recursion guard.
        # Set the value via the native setter (to the backend), 
        # always raise and lower the recursion shield
        #
        # If both are present, we can ignore *one* exception.  If one 
        # is present, exceptions are not ignored.
        #
        # always raise and lower the recursion shield.
        can_ignore = 0
        if all_setter and key_setter : can_ignore = 1

        if all_setter :
            try :
                d['_attributes'][key]['recursion'] = True
                all_setter (key, val)
            except Exception as e :
                print "setter exception: " + str(e)
                # ignoring failures from getter
                pass
            except Exception as e :
                can_ignore -= 1
                if not can_ignore : raise e
            finally :
                d['_attributes'][key]['recursion'] = False

        if key_setter :
            try :
                d['_attributes'][key]['recursion'] = True
                key_setter (val)
            except Exception as e :
                can_ignore -= 1
                if not can_ignore : raise e
            finally :
                d['_attributes'][key]['recursion'] = False



    ####################################
    def _attributes_t_call_getter (self, key) :
        """
        This internal function is not to be used by the consumer of this API.

        It triggers the invocation of any registered getter function, usually
        before an attribute's get()
        """

        # make sure interface is ready to use.
        d = self._attributes_t_init (key)

        # avoid recursion
        if d['_attributes'][key]['recursion'] :
            return

        # no callbacks for private keys
        if key[0] == '_' and d['_private'] :
            return

        # key getter overwrites results from all_getter
        all_getter = d['getter']
        key_getter = d['_attributes'][key]['getter']


        # # Note that attributes have a time-to-live (ttl).  If a _attributes_i_set
        # # operation is attempted within 'time-of-last-update + ttl', the operation
        # # is not triggering backend setter hooks, to avoid trashing (hooks are
        # # expected to be costly).  The force flag set to True will request to call 
        # # registered getter hooks even if ttl is not yet expired.
        # 
        # # NOTE: in Bliss, ttl does not make much sense, as this will only lead to
        # # valid attribute values if attribute changes are pushed from adaptor to
        # # API -- Bliss does not do that.
        # 
        # # For example, job.wait() will update the plugin level state to 'Done',
        # # but the cached job.state attribute will remain 'New' as the plugin does
        # # not push the state change upward
        #
        # age = self._attributes_t_get_age (key)
        # ttl = d['_attributes'][key]['ttl']
        #
        # if age < ttl :
        #     return


        # get the value from the native getter (from the backend), and
        # set it via the attribute setter.  The setter will not call
        # attrib setters or callbacks, due to the recursion guard.
        #
        # If both are present, we can ignore *one* exception.  If one 
        # is present, exceptions are not ignored.
        #
        # always raise and lower the recursion shield.
        can_ignore = 0
        if all_getter and key_getter : can_ignore = 1

        if all_getter :

            try :
                d['_attributes'][key]['recursion'] = True
                val=all_getter (key)
            except Exception as e :
                can_ignore -= 1
                if not can_ignore : raise e
            finally :
              d['_attributes'][key]['recursion'] = False

        if key_getter :
            try :
                d['_attributes'][key]['recursion'] = True
                key_getter ()
            except Exception as e :
                can_ignore -= 1
                if not can_ignore : raise e
            finally :
                d['_attributes'][key]['recursion'] = False



    ####################################
    def _attributes_t_call_lister (self) :
        """
        This internal function is not to be used by the consumer of this API.

        It triggers the invocation of any registered lister function, usually
        before a list() call.
        """

        # make sure interface is ready to use.
        d = self._attributes_t_init ()

        # avoid recursion
        if d['recursion'] :
            return

        lister = d['lister']

        if lister :

            # the lister is simply called, and it is expected that it internally
            # adds/removes attributes as needed.
            #
            # always raise and lower the recursion shield
            try :
                d['recursion'] = True
                return lister ()
            finally :
                d['recursion'] = False

        return []



    ####################################
    def _attributes_t_call_caller (self, key, id, cb) :
        """
        This internal function is not to be used by the consumer of this API.

        It triggers the invocation of any registered caller function, usually
        after an 'add_callback()' call.
        """

        # make sure interface is ready to use.
        d = self._attributes_t_init (key)

        # avoid recursion
        if d['recursion'] :
            return

        # no callbacks for private keys
        if key[0] == '_' and d['_private'] :
            return

        caller = d['caller']

        if caller :

            # the caller is simply called, and it is expected that it internally
            # adds/removes callbacks as needed
            #
            # always raise and lower the recursion shield
            try :
                d['recursion'] = True
                return caller (key, id, cb)
            finally :
                d['recursion'] = False

        return []



    ####################################
    def _attributes_t_underscore (self, key) :
        """ 
        This internal function is not to be used by the consumer of this API.

        The method accepts a CamelCased word, and translates that into
        'under_score' notation -- IFF '_camelcasing' is set

        Kudos: http://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-camel-case
        """

        # make sure interface is ready to use
        d = self._attributes_t_init ()

        _camel_case_regex_1 = re.compile('(.)([A-Z][a-z]+)')
        _camel_case_regex_2 = re.compile('([a-z0-9])([A-Z])')

        if d['_camelcasing'] :
            temp = Attributes._camel_case_regex_1.sub(r'\1_\2', key)
            return Attributes._camel_case_regex_2.sub(r'\1_\2', temp).lower()
        else :
            return key



    ####################################
    def _attributes_t_conversion (self, key, val) :
        """
        This internal function is not to be used by the consumer of this API.

        The method checks a given attribute value against the attribute's
        flags, and performs some simple type conversion as needed.  Also, the
        method will restore a 'None' value to the attribute's default value.

        A deriving class can add additional value checks for attributes by
        calling L{_attributes_check_add} (key, check, flow=flow).
        """

        # make sure interface is ready to use.  We do not check for keys, that
        # needs to be done in the calling method.  For example, on 'set', type
        # conversions will be performed, but the key will not exist previously.
        d = self._attributes_t_init ()

        # if the key is not known
        if not key in d['_attributes'] :
            # cannot handle unknown attributes.  Attributes which have been
            # registered earlier will be fine, as they have type information.
            return val

        # check if a value is given.  If not, revert to the default value
        # (if available)
        if val == None :
            if 'default' in d['_attributes'][key] :
                val = d['_attributes'][key]['default']


        # perform flavor and type conversion
        val = self._attributes_t_conversion_flavor (key, val)

        # apply all value checks on the conversion result
        for check in d['_attributes'][key]['checks'] :
            ret = check (key, val)
            if ret != True :
                raise BadParameter ("attribute value %s is not valid: %s"  %  (key, ret))

        # aaaand done
        return val


    ####################################
    def _attributes_t_conversion_flavor (self, key, val) :
        """ 
        This internal function is not to be used by the consumer of this API.
        This method should ONLY be called by _attributes_t_conversion! 
        """
        # FIXME: there are possibly nicer and more reversible ways to
        #        convert the flavors...

        # easiest conversion of them all... ;-)
        if val == None :
            return None

        # make sure interface is ready to use.
        d = self._attributes_t_init (key)

        # check if we need to serialize a list into a scalar
        f = d['_attributes'][key]['flavor']
        if f == VECTOR :
            # we want a vector
            if isinstance (val, list) :
                # val is already vec - apply type conversion on all elems
                ret = []
                for elem in val :
                    ret.append (self._attributes_t_conversion_type (key, elem))
                return ret
            else :
                # need to create vec from scalar
                if isinstance (val, basestring) :
                    # for string values, we split on white spaces and type-convert 
                    # all elements
                    vec = val.split ()
                    ret = []
                    for element in vec :
                        ret.append (self._attributes_t_conversion_type (key, element))
                    return ret
                else :   
                    # all non-string types are interpreted as only element of
                    # a single-member list
                    return [self._attributes_t_conversion_type (key, val)]


        elif f == SCALAR :
            # we want a scalar
            if isinstance (val, list) :
                # need to create scalar from vec
                if len (val) > 1 :
                    # if the list has more than one element, we use an intermediate
                    # string representation of the list before converting to a scalar
                    # This is the weakest conversion mode, and will not very
                    # likely yield useful results.
                    tmp = ""
                    for i in val :
                        tmp += str(i) + " "
                    return self._attributes_t_conversion_type (key, tmp)
                elif len (val) == 1 :
                    # for single element lists, we simply use the one element as
                    # scalar value 
                    return self._attributes_t_conversion_type (key, val[0])
                else :
                    # no value in list
                    return None
            else :
                # scalar is already scalar, just do type conversion
                return self._attributes_t_conversion_type (key, val)


        # we should never get here...
        raise NoSuccess ("Cannot evaluate attribute flavor (%s) : %s"  %  (key, str(f)))


    ####################################
    def _attributes_t_conversion_type (self, key, val) :
        """ 
        This internal function is not to be used by the consumer of this API.
        This method should ONLY be called by _attributes_t_conversion!
        """

        # make sure interface is ready to use.
        d = self._attributes_t_init (key)

        # oh python, how about a decent switch statement???
        t   = d['_attributes'][key]['type']
        ret = None
        try :
            # FIXME: add time/date conversion to/from string
            if   t == ANY    : return        val  
            elif t == INT    : return int   (val) 
            elif t == FLOAT  : return float (val) 
            elif t == BOOL   : return bool  (val) 
            elif t == STRING : return str   (val) 
            else             : return        val  
        except ValueError as e:
            raise BadParameter ("attribute value %s has incorrect type: %s" %  (key, val))

        # we should never get here...
        raise NoSuccess ("Cannot evaluate attribute type (%s) : %s"  %  (key, str(t)))

    ####################################
    def _attributes_t_wildcard2regex (self, pattern) :
        """ 
        This internal function is not to be used by the consumer of this API.

        This method converts a string containing POSIX shell wildcards into
        a regular expression with the same matching properties::

            *       -> .*
            ?       -> .
            {a,b,c} -> (a|b|c)
            [abc]   -> [abc]   
            [!abc]  -> [^abc]   
        """

        re = pattern

        re.replace ('*', '.*')  # set of characters
        re.replace ('?', '.' )  # single character

        # character classes
        match = re.find ('[', 0)
        while match >= 0 :
            if  re[first + 1] == '!' :
                re[first + 1] =  '^'
            match = re.find ('[', match + 1)

        # find opening { and closing }
        first = re.find ('{', 0)
        last  = re.find ('}', first + 1)

        # while match
        while first >= 0 and last  >= 0 :
            # replace with ()
            re[first] = '('
            re[last] = '('

            # also, replace all ',' with with '|' for alternatives
            comma = re.find (',', first)
            while comma >= 0 :
                re[comma] = '|'
                comma = re.find (',', comma + 1)

            # done - find next bracket pair...
            first = re.find ('{', last + 1)
            last  = re.find ('}', first + 1)


    ####################################
    def _attributes_t_get_age (self, key) :
        """ get the age of the attribute, i.e. seconds.microseconds since last set """

        # make sure interface is ready to use.
        d    = self._attributes_t_init (key)
        last = d['_attributes'][key]['last']
        age  = now() - last

        return (age.microseconds + (age.seconds + age.days * 24 * 3600) * 1e6) / 1e6



    ###########################################################################
    #
    # internal interface
    #
    # This internal interface is used by the public interfaces (dict,
    # properties, GFD.90).  We assume that CamelCasing and under_scoring is
    # sorted out before this internal interface is called.  All other tests,
    # verifications, and conversion are done here though.
    #
    # Naming: __attributes_i*
    #
    ####################################
    def _attributes_i_set (self, key, val=None, force=False, flow=_DOWN) :
        """
        This internal method should not be explicitly called by consumers of
        this API, but is indirectly used via the different public interfaces.

        See L{set_attribute} (key, val) for details.

        New value checks can be added dynamically, and per attribute, by calling
        L{_attributes_check_add} (key, callable).

        Some internal methods can set the 'force' flag, and will be able to set
        attributes even in ReadOnly mode.  That is, for example, used for getter
        hooks.  Note that the Final flag will be honored even if Force is set,
        and will result in the set request being ignored.
        """

        # make sure interface is ready to use
        d = self._attributes_t_init ()

        # if the key is not known
        if not key in d['_attributes'] :
            if key[0] == '_' and d['_private'] :
                # if the set is private, we can register the new key.  It
                # won't have any callbacks at this point.
                self._attributes_register (key, None, ANY, SCALAR, WRITEABLE,
                        EXTENDED, flow=flow)

            elif flow==self._UP or d['_extensible'] :
                # if the set is extensible, we can register the new key.  It
                # won't have any callbacks at this point.
                self._attributes_register (key, None, ANY, SCALAR, WRITEABLE,
                        EXTENDED, flow=flow)

            else :
                # we cannot add new keys on non-extensible / non-private sets
                raise IncorrectState ("attribute set is not extensible/private (key %s)" %  key)


        # known attribute
        else :

            # check if we are allowed to change the attribute - complain if not.
            # Also, simply ignore write attempts to finalized keys.
            if 'mode' in  d['_attributes'][key] :

                mode = d['_attributes'][key]['mode']

                if FINAL == mode :
                    return

                elif READONLY == mode :
                    if not force :
                        raise BadParameter ("attribute %s is not writeable" %  key)


        # permissions are confirmed, set the attribute with conversion etc.

        # apply any attribute conversion
        val = self._attributes_t_conversion (key, val)

        # make sure the key's value entry exists
        if not 'value' in d['_attributes'][key] :
            d['_attributes'][key]['value'] = None

        # only once an attribute is explicitly set, it 'exists' for the purpose
        # of the 'attribute_exists' call, and the key iteration
        d['_attributes'][key]['exists'] = True

        # # only actually change the attribute when the new value differs --
        # # and only then invoke any callbacks and hooked setters
        # if val != d['_attributes'][key]['value'] :
        #
        # NOTE: this check is disabled now: we certainly want to update 'last',
        # and IMHO that should also imply a notification call, etc.  FWIW, the
        # spec is inconclusive here.
        #
        # if val != d['_attributes'][key]['value'] :


        d['_attributes'][key]['value'] = val
        d['_attributes'][key]['last']  = now ()

        if flow==self._DOWN :
            self._attributes_t_call_setter (key, val)

        self._attributes_t_call_cb (key)


    ####################################
    def _attributes_i_get (self, key, flow) :
        """
        This internal method should not be explicitly called by consumers of
        this API, but is indirectly used via the different public interfaces.

        see L{get_attribute} (key) for details.

        Note that this method is not performing any checks or conversions --
        those are all performed when *setting* an attribute.  So, any attribute
        flags (type, mode, flavor) are evaluated on setting, not on getting.
        This implementation does not account for resulting race conditions
        (changing attribute types after setting for example) -- but the public
        API does not allow that anyway.
        """

        # make sure interface is ready to use
        d = self._attributes_t_init (key)

        self._attributes_t_call_getter (key)

        if 'value' in d['_attributes'][key] :
            return d['_attributes'][key]['value']

        if 'default' in d['_attributes'][key] :
            return d['_attributes'][key]['default']
                
        return None



    ####################################
    def _attributes_i_list (self, ext=True, priv=False, CamelCase=True, flow=_DOWN) :
        """
        This internal method should not be explicitly called by consumers of
        this API, but is indirectly used via the different public interfaces.

        see L{list_attributes} () for details.

        Note that registration alone does not qualify for listing.  If 'ext' is
        True (default),extended attributes are listed, too.
        """

        # make sure interface is ready to use
        d = self._attributes_t_init ()

        # call list hooks to update state for listing
        self._attributes_t_call_lister ()

        ret = []
        for key in sorted(d['_attributes'].iterkeys()) :
            if d['_attributes'][key]['mode'] != ALIAS :
                if d['_attributes'][key]['exists'] :

                    e = d['_attributes'][key]['extended'] 
                    p = d['_attributes'][key]['private'] 
                    k = key

                    if CamelCase :
                        k = d['_attributes'][key]['camelcase']

                    if e and ext :
                        if p and priv :
                            ret.append (k)
                        elif not p :
                            ret.append (k)
                    elif not e :
                        if p and priv :
                            ret.append (k)
                        elif not p :
                            ret.append (k)

        return ret


    ####################################
    def _attributes_i_find (self, pattern, flow) :
        """
        This internal method should not be explicitly called by consumers of
        this API, but is indirectly used via the different public interfaces.

        see L{find_attributes} (pattern) for details.
        """

        # FIXME: wildcard-to-regex

        # make sure interface is ready to use
        d = self._attributes_t_init ()


        # separate key and value pattern
        p_key  = ""    # string pattern
        p_val  = ""    # string pattern
        pc_key = None  # compiled pattern
        pc_val = None  # compiled pattern


        if pattern[0] == '=' :
            # no key pattern present, only grep on values
              p_val = self._attributes_t_wildcard2regex (pattern[1:])
        else :
          p = re.compile (r'[^\]=')
          tmp = p.split (pattern, 2)  # only split on first '='
          
          if len (tmp) >  0 : 
              # at least one elem: only key pattern present
              p_key = self._attributes_t_wildcard2regex (tmp[0])
          
          if len (tmp) == 2 :
              # two elems: val pattern is also present
              p_val = self._attributes_t_wildcard2regex (tmp[1])

        # compile the found pattern
        if len (p_key) : pc_key = re.compile (p_key)
        if len (p_val) : pc_val = re.compile (p_val)

        # now dig out matching keys. List hooks are triggered in
        # _attributes_i_list(flow).
        matches = []
        for key in self._attributes_i_list (flow) :
            val = str(self._attributes_i_get (key, flow=flow))

            if ( (pc_key == None) or pc_key.search (key) ) and \
               ( (pc_val == None) or pc_val.search (val) )     :
                matches.append (key)

        return matches


    ####################################
    def _attributes_i_exists (self, key, flow) :
        """
        This internal method should not be explicitly called by consumers of
        this API, but is indirectly used via the different public interfaces.

        see L{attribute_exists} (key) for details.

        Registered keys which have never been explicitly set to a value do not
        exist for the purpose of this call.
        """

        # make sure interface is ready to use
        d = self._attributes_t_init ()

        # check if we know about that attribute
        if 'exists' in d['_attributes'][key] :
            if  d['_attributes'][key]['exists'] :
                return True

        return False


    ####################################
    def _attributes_i_is_extended (self, key, flow) :
        """
        This internal method should not be explicitly called by consumers of
        this API, but is indirectly used via the different public interfaces.

        This method will check if the given key is extended, i.e. was registered
        on the fly, vs. registered explicitly.

        This method is not used by, and not exposed via the public API, yet.
        """

        # make sure interface is ready to use
        d = self._attributes_t_init (key)

        return d['_attributes'][key]['extended']


    ####################################
    def _attributes_i_is_private (self, key, flow) :
        """
        This internal method should not be explicitly called by consumers of
        this API, but is indirectly used via the different public interfaces.

        This method will check if the given key is private, i.e. starts with an
        underscore and 'allow_private' is enabled.

        This method is not used by, and not exposed via the public API, yet.
        """

        # make sure interface is ready to use
        d = self._attributes_t_init (key)

        return d['_attributes'][key]['private']


    ####################################
    def _attributes_i_is_readonly (self, key, flow) :
        """
        This internal method should not be explicitly called by consumers of
        this API, but is indirectly used via the different public interfaces.

        see L{attribute_is_readonly} (key) for details.

        This method will check if the given key is readonly, i.e. cannot be
        'set'.  The call will also return 'True' if the attribute is final
        """

        # make sure interface is ready to use
        d = self._attributes_t_init (key)

        # check if we know about that attribute
        if  d['_attributes'][key]['mode'] == FINAL or \
            d['_attributes'][key]['mode'] == READONLY :
            return True

        return False


    ####################################
    def _attributes_i_is_writeable (self, key, flow) :
        """
        This internal method should not be explicitly called by consumers of
        this API, but is indirectly used via the different public interfaces.

        see L{attribute_is_writeable} (key) for details.

        This method will check if the given key is writeable - i.e. not readonly.
        """

        return not self._attributes_i_is_readonly (key, flow=flow)


    ####################################
    def _attributes_i_is_removable (self, key, flow) :
        """
        This internal method should not be explicitly called by consumers of
        this API, but is indirectly used via the different public interfaces.

        see L{attribute_is_removable} (key) for details.

        'True' if the attrib is writeable and Extended.
        """

        if self._attributes_i_is_writeable (key, flow=flow) and \
           self._attributes_i_is_extended  (key, flow=flow)     :
            return True

        return False

    ####################################
    def _attributes_i_is_vector (self, key, flow) :
        """
        This internal method should not be explicitly called by consumers of
        this API, but is indirectly used via the different public interfaces.

        see L{attribute_is_vector} (key) for details.
        """

        # make sure interface is ready to use
        d = self._attributes_t_init (key)

        # check if we know about that attribute
        if  d['_attributes'][key]['flavor'] == VECTOR :
            return True

        return False


    ####################################
    def _attributes_i_is_final (self, key, flow) :
        """
        This internal method should not be explicitly called by consumers of
        this API, but is indirectly used via the different public interfaces.

        This method will query the 'final' flag for an attribute, which signals
        that the attribute will never change again.

        This method is not used by, and not exposed via the public API, yet.
        """

        # make sure interface is ready to use
        d = self._attributes_t_init (key)

        if FINAL == d['_attributes'][key]['mode'] :
             return True

        # no final flag found -- assume non-finality!
        return False


    ####################################
    def _attributes_i_add_cb (self, key, cb, flow) :
        """
        This internal method should not be explicitly called by consumers of
        this API, but is indirectly used via the different public interfaces.

        see L{add_callback} (key, cb) for details.
        """

        # make sure interface is ready to use
        d = self._attributes_t_init (key)

        d['_attributes'][key]['callbacks'].append (cb)

        id = len (d['_attributes'][key]['callbacks']) - 1

        if flow==self._DOWN :
            self._attributes_t_call_caller (key, id, cb)

        return id


    ####################################
    def _attributes_i_del_cb (self, key, id=None, flow=_DOWN) :
        """
        This internal method should not be explicitly called by consumers of
        this API, but is indirectly used via the different public interfaces.

        see L{remove_callback} (key, cb) for details.
        """

        # make sure interface is ready to use
        d = self._attributes_t_init (key)

        if flow==self._DOWN :
            self._attributes_t_call_caller (key, id, None)

        # id == None: remove all callbacks
        if not id :
            d['_attributes'][key]['callbacks'] = []
        else :
            if len (d['_attributes'][key]['callbacks']) < id :
                raise BadParameter ("invalid callback cookie for attribute %s"  %  key)
            else :
                # do not pop from list, that would invalidate the id's!
                d['_attributes'][key]['callbacks'][id] = undef



    ############################################################################
    #
    # This part of the interface is primarily for use in deriving classes, which
    # thus provide the Attributes interface.
    #
    # Keys should be provided as CamelCase (only relevant if camelcasing is
    # set).
    #
    # Naming: _attributes*
    #
    ####################################
    def _attributes_register (self, key, default=None, typ=ANY, flavor=SCALAR,
                              mode=WRITEABLE, ext=False, flow=_DOWN) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        Register a new attribute.

        This function ignores extensible, private, final and readonly flags.  It
        can also be used to re-register an existing attribute with new
        properties -- the old attribute value, callbacks etc. will be lost
        though.  Using this call that way may result in confusing behaviour on
        the public API level.
        """
        # FIXME: check for valid mode and flavor settings

        # make sure interface is ready to use
        d = self._attributes_t_init ()

        priv = False
        if d['_private'] and key[0] == '_' :
            priv = True

        # we expect keys to be registered as CamelCase (in those cases where
        # that matters).  But we store attributes in 'under_score' version.
        us_key = self._attributes_t_underscore (key)

        # remove any old instance of this attribute
        if us_key in  d['_attributes'] :
            self._attributes_unregister (us_key, flow=flow)

        # register the attribute and properties
        d['_attributes'][us_key]                 = {}
        d['_attributes'][us_key]['value']        = default # initial value
        d['_attributes'][us_key]['default']      = default # default value
        d['_attributes'][us_key]['type']         = typ     # int, float, enum, ...
        d['_attributes'][us_key]['exists']       = False   # no value set, yet
        d['_attributes'][us_key]['flavor']       = flavor  # scalar / vector
        d['_attributes'][us_key]['mode']         = mode    # readonly / writeable / final
        d['_attributes'][us_key]['extended']     = ext     # is an extended attribute 
        d['_attributes'][us_key]['private']      = priv    # is a  private attribute
        d['_attributes'][us_key]['camelcase']    = key     # keep original key name
        d['_attributes'][us_key]['underscore']   = us_key  # keep under_scored name
        d['_attributes'][us_key]['enums']        = []      # list of valid enum values
        d['_attributes'][us_key]['checks']       = []      # list of custom value checks
        d['_attributes'][us_key]['callbacks']    = []      # list of callbacks
        d['_attributes'][us_key]['recursion']    = False   # recursion check for callbacks
        d['_attributes'][us_key]['setter']       = None    # custom attribute setter
        d['_attributes'][us_key]['getter']       = None    # custom attribute getter
        d['_attributes'][us_key]['last']         = never   # time of last refresh (never)
        d['_attributes'][us_key]['ttl']          = 0.0     # refresh delay (none)

        # for enum types, we add a value checker
        if typ == ENUM :
            ######################################
            def _enum_check (key, val) :
                if None == val  :
                    # None is always allowed
                    return True
                
                us_key = self._attributes_t_underscore (key)
                d      = self._attributes_t_init       (us_key)

                vals   = d['_attributes'][us_key]['enums']

                # check if there is anything to check
                if not vals :
                    return True
                
                # value must be one of allowed enums
                if val in vals :
                        return True

                # Houston, we got a problem...
                return """
                incorrect value (%s) for Enum typed attribute (%s).
                Allowed values: %s
                """  %  (str(val), key, str(vals))
            ######################################

            self._attributes_check_add (key, _enum_check, flow=flow)



    ####################################
    def _attributes_register_deprecated (self, key, alias, flow) :
        """
        Often enough, there is the need to use change attribute names.  It is
        good practice to not simply rename attributes, and thus effectively
        remove old ones, as that is likely to break existing code.  Instead, new
        names are added, and old names are kept for a certain time for backward
        compatibility.  To support migration to the new names, the old names
        should be marked as 'deprecated' though - which can be configured to
        print a warning whenever an old, deprecated attribute is used.

        This method allows to register such deprecated attribute names.  They
        can thus be used just like new ones, and in fact are implemented as
        aliases to the new ones -- but they will print a deprecated warning on
        usage.

        The first parameter is the old name of the attribute, the second
        parameter is the aliased new name.  Note that the new name needs to be
        registered before (via L{_attributes_register})::

            # old code:
            self._attributes_register ('apple', 'Appel', STRING, SCALAR, WRITEABLE)

            # new code
            self._attributes_register ('fruit', 'Appel', STRING, SCALAR, WRITEABLE)
            self._attributes_register_deprecated ('apple', 'fruit)

        In some cases, you may want to deprecate a variable and not replace it
        with a new one.  In order to keep this interface simple, this can be
        achieved via::

            # new code
            self._attributes_register ('deprecated_apple', 'Appel', STRING, SCALAR, WRITEABLE)
            self._attributes_register_deprecated ('apple', 'deprecated_apple)

        This way, the user will either see a warning, or has to explicitly use
        'deprecated_apple' as attribute name -- which should be warning enough,
        at least for the programmer ;o)
        """

        # we expect keys to be registered as CamelCase (in those cases where
        # that matters).  But we store attributes in 'under_score' version.
        us_alias = self._attributes_t_underscore (alias)
        us_key   = self._attributes_t_underscore (key)

        # make sure interface is ready to use
        # This check will throw if 'alias' was not registered before.
        d = self._attributes_t_init (us_alias)

        # remove any old instance of this attribute
        if us_key in  d['_attributes'] :
            self._attributes_unregister (us_key, flow=flow)

        # register the attribute and properties
        d['_attributes'][us_key]               = {}
        d['_attributes'][us_key]['mode']       = ALIAS      # alias
        d['_attributes'][us_key]['alias']      = us_alias   # aliased var
        d['_attributes'][us_key]['camelcase']  = key        # keep original key name
        d['_attributes'][us_key]['underscore'] = us_key     # keep under_scored name



    ####################################
    def _attributes_unregister (self, key, flow) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        Unregister an attribute.

        This function ignores the extensible, private, final and readonly flag,
        and is supposed to be used by derived classes, not by the consumer of
        the API.

        Note that unregistering is different from setting the value to 'None' --
        all meta information about the attribute will be removed.  Further
        attempts to access the attribute from the public API will result in an
        DoesNotExist exception.  This method should be used sparingly -- in
        fact, GFD.90 requires final attributes to stay around forever (frozen).
        """

        # make sure interface is ready to use
        us_key = self._attributes_t_underscore (key)
        d      = self._attributes_t_init       (us_key)

        # if the attribute exists, purge it
        if us_key in d['_attributes'] :
            del (d['_attributes'][us_key])


    ####################################
    def _attributes_remove (self, key, flow) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        Remove an extended an attribute.

        This function allows to safely remove any attribute which is 'private'
        or 'extended' and has write permissions.
        """

        # make sure interface is ready to use
        us_key = self._attributes_t_underscore (key)
        d      = self._attributes_t_init       (us_key)

        if self._attributes_i_is_removable (key, flow=flow) :
            del (d['_attributes'][us_key])


    ####################################
    def _attributes_set_enums (self, key, enums=None, flow=_DOWN) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        Specifies the set of allowed values for Enum typed attributes.  If not
        set, or if list is None, any values are allowed.
        """

        us_key = self._attributes_t_underscore (key)
        d      = self._attributes_t_init       (us_key)

        d['_attributes'][us_key]['enums'] = enums


    ####################################
    def _attributes_extensible (self, e=True, 
                                getter=None, setter=None, 
                                lister=None, caller=None, 
                                flow=_DOWN) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        Allow (or forbid) the on-the-fly creation of new attributes.  Note that
        this method also allows to *remove* the extensible flag -- that leaves
        any previously created extended attributes untouched, but just prevents
        the creation of new extended attributes.
        """

        d = self._attributes_t_init ()
        d['_extensible'] = e

        if getter : self._attributes_set_global_getter (getter, flow=flow)
        if setter : self._attributes_set_global_setter (setter, flow=flow)
        if lister : self._attributes_set_global_lister (lister, flow=flow)
        if caller : self._attributes_set_global_caller (caller, flow=flow)



    ####################################
    def _attributes_allow_private (self, p=True, flow=_DOWN) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        Allow (or forbid) the on-the-fly creation of private attributes
        (starting with underscore).  Note that this method also allows to
        *remove* the respective flag -- that leaves any previously created
        private attributes untouched, but just prevents the creation of new
        private attributes.
        """

        d = self._attributes_t_init ()
        d['_private'] = p


    ####################################
    def _attributes_camelcasing (self, c=True, flow=_DOWN) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        Use 'CamelCase' for dict entries and the GFD.90 API, but 'under_score'
        for properties.

        Note that we do not provide an option to turn CamelCasing off - once it
        is turned on, it stays on -- otherwise we would loose attributes...
        """

        d = self._attributes_t_init ()
        d['_camelcasing'] = c


    ####################################
    def _attributes_deep_copy (self, other, flow=_DOWN) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        This method can be used to make sure that deep copies of derived classes
        are also deep copies of the respective attributes.  In accordance with
        GFD.90, the deep copy will ignore callbacks.  It will copy checks
        though, as the assumption is that value constraints stay valid.
        """

        
        # make sure interface is ready to use
        d = self._attributes_t_init ()

        other_d = {}

        # for some reason, deep copy won't work on the '_attributes' dict, so we
        # do it manually.  Use the list copy c'tor to copy list elements.
        other_d['_camelcasing'] = d['_camelcasing']
        other_d['_attributes']  = d['_attributes'] 
        other_d['_extensible']  = d['_extensible'] 
        other_d['_private']     = d['_private']
        other_d['_camelcasing'] = d['_camelcasing']
        other_d['recursion']    = d['recursion']    
        other_d['getter']       = d['setter']    
        other_d['setter']       = d['setter']    
        other_d['lister']       = d['lister']    
        other_d['caller']       = d['caller']    

        other_d['_attributes'] = {}

        for key in d['_attributes'] :
            other_d['_attributes'][key] = {}
            other_d['_attributes'][key]['default']      =       d['_attributes'][key]['default']   
            other_d['_attributes'][key]['exists']       =       d['_attributes'][key]['exists']      
            other_d['_attributes'][key]['type']         =       d['_attributes'][key]['type']      
            other_d['_attributes'][key]['flavor']       =       d['_attributes'][key]['flavor']    
            other_d['_attributes'][key]['mode']         =       d['_attributes'][key]['mode']      
            other_d['_attributes'][key]['extended']     =       d['_attributes'][key]['extended']  
            other_d['_attributes'][key]['private']      =       d['_attributes'][key]['private']  
            other_d['_attributes'][key]['camelcase']    =       d['_attributes'][key]['camelcase'] 
            other_d['_attributes'][key]['underscore']   =       d['_attributes'][key]['underscore']
            other_d['_attributes'][key]['enums']        = list (d['_attributes'][key]['enums'])
            other_d['_attributes'][key]['checks']       = list (d['_attributes'][key]['checks'])
            other_d['_attributes'][key]['callbacks']    = list (d['_attributes'][key]['callbacks'])
            other_d['_attributes'][key]['recursion']    =       d['_attributes'][key]['recursion']
            other_d['_attributes'][key]['setter']       =       d['_attributes'][key]['setter']
            other_d['_attributes'][key]['getter']       =       d['_attributes'][key]['getter']
            other_d['_attributes'][key]['last']         =       d['_attributes'][key]['last']
            other_d['_attributes'][key]['ttl']          =       d['_attributes'][key]['ttl']

            if d['_attributes'][key]['flavor'] == VECTOR and \
               d['_attributes'][key]['value' ] != None       :
                other_d['_attributes'][key]['value']  = list (d['_attributes'][key]['value'])
            else :
                other_d['_attributes'][key]['value']  =       d['_attributes'][key]['value']

        # set the new dictionary as state for copied class
        _AttributesBase.__setattr__ (other, '_d', other_d)


    ####################################
    def _attributes_dump (self, msg=None, flow=_DOWN) :
        """ 
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        Debugging dump to stdout.
        """

        # make sure interface is ready to use
        d = self._attributes_t_init ()


        keys_all   = sorted (d['_attributes'].iterkeys ())
        keys_exist = (key.lower for key in sorted (self._attributes_i_list (flow)))

        print "---------------------------------------"
        print str (type (self))

        if msg :
            print "---------------------------------------"
            print msg

        print "---------------------------------------"
        print " %-30s : %s"  %  ("Extensible"  , d['_extensible'])
        print " %-30s : %s"  %  ("Private"     , d['_private'])
        print " %-30s : %s"  %  ("CamelCasing" , d['_camelcasing'])
        print "---------------------------------------"

        print "'Registered' attributes"
        for key in keys_all :
            if key not in keys_exist :
                if not  d['_attributes'][key]['mode'] == ALIAS and \
                   not  d['_attributes'][key]['extended'] :
                    print " %-30s [%6s, %6s, %9s, %3d]: %s"  % \
                             (d['_attributes'][key]['camelcase'],
                              d['_attributes'][key]['type'],
                              d['_attributes'][key]['flavor'],
                              d['_attributes'][key]['mode'],
                      len(d['_attributes'][key]['callbacks']),
                              d['_attributes'][key]['value']
                              )

        print "---------------------------------------"

        print "'Existing' attributes"
        for key in keys_exist :
            if not  d['_attributes'][key]['mode'] == ALIAS :
                print " %-30s [%6s, %6s, %9s, %3d]: %s"  % \
                         (d['_attributes'][key]['camelcase'],
                          d['_attributes'][key]['type'],
                          d['_attributes'][key]['flavor'],
                          d['_attributes'][key]['mode'],
                      len(d['_attributes'][key]['callbacks']),
                          d['_attributes'][key]['value']
                          )

        print "---------------------------------------"

        print "'Extended' attributes"
        for key in keys_all :
            if key not in keys_exist :
                if not  d['_attributes'][key]['mode'] == ALIAS and \
                        d['_attributes'][key]['extended'] :
                    print " %-30s [%6s, %6s, %9s, %3d]: %s"  % \
                             (d['_attributes'][key]['camelcase'],
                              d['_attributes'][key]['type'],
                              d['_attributes'][key]['flavor'],
                              d['_attributes'][key]['mode'],
                      len(d['_attributes'][key]['callbacks']),
                              d['_attributes'][key]['value']
                              )

        print "---------------------------------------"

        print "'Deprecated' attributes (aliases)"
        for key in keys_all :
            if key not in keys_exist :
                if d['_attributes'][key]['mode'] == ALIAS :
                    print " %-30s [%24s]:  %s"  % \
                             (d['_attributes'][key]['camelcase'],
                              ' ',
                              d['_attributes'][key]['alias']
                              )

        print "---------------------------------------"


    ####################################
    def _attributes_set_final (self, key, val=None, flow=_DOWN) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        This method will set the 'final' flag for an attribute, signalling that
        the attribute will never change again.  The ReadOnly flag is ignored.
        A final value can optionally be provided -- otherwise the attribute is
        frozen with its current value.

        Note that attributes_set_final() will trigger callbacks, even if the
        value was not set, or did not change.
        """

        # make sure interface is ready to use
        us_key = self._attributes_t_underscore (key)
        d      = self._attributes_t_init       (us_key)

        newval = val
        oldval = d['_attributes'][us_key]['value']
        if None == newval :
            # freeze at current value unless indicated otherwise
            val = oldval

        # flag as final, and set the final value (this order to avoid races in
        # callbacks)
        d['_attributes'][us_key]['mode'] = FINAL
        self._attributes_i_set (us_key, val, flow=flow)

        # callbacks are not invoked if the value did not change -- we take care
        # of that here.
        #
        # if  None == newval or oldval == newval :

        self._attributes_t_call_cb (key)


    ####################################
    def _attributes_set_ttl (self, key, ttl=0.0, flow=_DOWN) :
        """ set attributes TTL in seconds (float) -- see L{_attributes_i_set} """

        # make sure interface is ready to use.
        us_key = self._attributes_t_underscore (key)
        d      = self._attributes_t_init       (us_key)

        d['_attributes'][us_key]['ttl'] = ttl



    ####################################
    def _attributes_check_add (self, key, check, flow=_DOWN) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        Value checks can be added dynamically, and per attribute.  'callable'
        needs to be a python callable, and will be invoked as::

            callable (key, val)

        Those checks will be invoked whenever a new attribute value is set.  If
        that call then returns 'True', the value is accepted.  Otherwise, the
        value will be considered to be invalid, which results in an exception as
        per above.  'callable' can return a string as error message.  
        """

        # make sure interface is ready to use
        us_key = self._attributes_t_underscore (key)
        d = self._attributes_t_init (us_key)

        # register the attribute and properties
        d['_attributes'][us_key]['checks'].append (check)


    ####################################
    def _attributes_set_getter (self, key, getter, flow=_DOWN) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        The Attribute API makes no assumptions on how attribute values are kept
        up-to-date.  In general, it expects an asynchronous thread to set
        attribute values as needed.  To keep the complexity low for backend
        developers, it also supports the registration of 'setter' and 'getter'
        hooks.  Those are very similar to callbacks, but kind of inversed: where
        frontend callbacks are invoked on backend attribute changes, backend
        hooks are invoked on frontend attribute queries.  They are expected to
        internally trigger state updates.

        For example, on::

            print c.attrib

        The attribute getter for the 'attrib' attribute will be invoked.  If for
        that attribute a getter hook is registered, that hook will first query
        the backend for value updates.  After that update has been performed,
        the getter will retrieve the (updated) value.

        Similarly, setter hooks will be invoked *after* the attribute setter
        method, to inform the implementation of the updated attribute value.

        Further, list hooks are invoked before a list or find operation is
        really internally executed, to allow the implementation to updated the
        list of available attributes.  
        
        Note that only one setter/getter/lister method can be registered (for
        setters/getters per key, for listers per class instance).

        Hooks have a different call signature than callbacks::
        
            setter        (self, value)
            getter        (self)
            global_setter (self, key, value)
            global_setter (self, key)
            global_lister (self)
            global_caller (self, key)

        FIXME: consider a cooling-off period for caching.
        """

        # make sure interface is ready to use
        us_key = self._attributes_t_underscore (key)
        d      = self._attributes_t_init       (us_key)

        # register the attribute and properties
        d['_attributes'][us_key]['getter'] = getter


    ####################################
    def _attributes_set_setter (self, key, setter, flow=_DOWN) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        See documentation of L{_attributes_set_getter } for details.
        """

        # make sure interface is ready to use
        us_key = self._attributes_t_underscore (key)
        d      = self._attributes_t_init       (us_key)

        # register the attribute and properties
        d['_attributes'][us_key]['setter'] = setter


    ####################################
    def _attributes_set_global_lister (self, lister, flow) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        See documentation of L{_attributes_set_getter } for details.
        """

        d = self._attributes_t_init ()

        # register the attribute and properties
        d['lister'] = lister


    ####################################
    def _attributes_set_global_caller (self, caller, flow) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        See documentation of L{_attributes_set_getter } for details.
        """

        d = self._attributes_t_init ()

        # register the attribute and properties
        d['caller'] = caller


    ####################################
    def _attributes_set_global_getter (self, getter, flow) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        See documentation of L{_attributes_set_getter } for details.
        """

        d = self._attributes_t_init ()

        # register the attribute and properties
        d['getter'] = getter


    ####################################
    def _attributes_set_global_setter (self, setter, flow) :
        """
        This interface method is not part of the public consumer API, but can
        safely be called from within derived classes.

        See documentation of L{_attributes_set_getter } for details.
        """

        d = self._attributes_t_init ()

        # register the attribute and properties
        d['setter'] = setter


    ###########################################################################
    #
    # the GFD.90 attribute interface
    #
    # The GFD.90 interface supports CamelCasing, and thus converts all keys to
    # underscore before using them.
    # 
    ####################################
    def set_attribute (self, key, val, _flow=_DOWN) :
        """
        set_attribute(key, val)

        This method sets the value of the specified attribute.  If that
        attribute does not exist, DoesNotExist is raised -- unless the attribute
        set is marked 'extensible' or 'private'.  In that case, the attribute is
        created and set on the fly (defaulting to mode=writeable, flavor=Scalar,
        type=ANY, default=None).  A value of 'None' may reset the attribute to
        its default value, if such one exists (see documentation).

        Note that this method is performing a number of checks and conversions,
        to match the value type to the attribute properties (type, mode, flavor).
        Those conversions are not guaranteed to yield the expected result -- for
        example, the conversion from 'scalar' to 'vector' is, for complex types,
        ambiguous at best, and somewhat stupid.  The consumer of the API SHOULD
        ensure correct attribute values.  The conversions are intended to
        support the most trivial and simple use cases (int to string etc).
        Failed conversions will result in an BadParameter exception.

        Attempts to set a 'final' attribute are silently ignored.  Attempts to
        set a 'readonly' attribute will result in an IncorrectState exception
        being raised.

        Note that set_attribute() will trigger callbacks, if a new value
        (different from the old value) is given.  
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return   self._attributes_i_set        (us_key, val, flow=_flow)


    ####################################
    def get_attribute (self, key, _flow=_DOWN) :
        """
        get_attribute(key)

        This method returns the value of the specified attribute.  If that
        attribute does not exist, an DoesNotExist is raised.  It is not an
        error to query an existing, but unset attribute though -- that will
        result in 'None' to be returned (or the default value, if available).
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return   self._attributes_i_get        (us_key, _flow)


    ####################################
    def set_vector_attribute (self, key, val, _flow=_DOWN) :
        """
        set_vector_attribute (key, val)

        See also: L{set_attribute} (key, val).

        As python can handle scalar and vector types transparently, this method
        is in fact not very useful.  For that reason, it maps internally to the
        set_attribute method.
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return   self._attributes_i_set        (us_key, val, _flow)


    ####################################
    def get_vector_attribute (self, key, _flow=_DOWN) :
        """
        get_vector_attribute (key)

        See also: L{get_attribute} (key).

        As python can handle scalar and vector types transparently, this method
        is in fact not very useful.  For that reason, it maps internally to the
        get_attribute method.
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return   self._attributes_i_get        (us_key, _flow)


    ####################################
    def remove_attribute (self, key, _flow=_DOWN) :
        """
        remove_attribute (key)

        Removing an attribute is actually different from unsetting it, or from
        setting it to 'None'.  On remove, all traces of the attribute are
        purged, and the key will not be listed on L{list_attributes}() anymore.
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return   self._attributes_remove       (us_key, _flow)


    ####################################
    def list_attributes (self, _flow=_DOWN) :
        """
        list_attributes ()

        List all attributes which have been explicitly set. 
        """

        return self._attributes_i_list (_flow)


    ####################################
    def find_attributes (self, pattern, _flow=_DOWN) :
        """
        find_attributes (pattern)

        Similar to list(), but also grep for a given attribute pattern.  That
        pattern is of the form 'key=val', where both 'key' and 'val' can contain
        POSIX shell wildcards.  For non-string typed attributes, the pattern is
        applied to a string serialization of the typed value, if that exists.
        """

        return self._attributes_i_find (pattern, _flow)


    ####################################
    def attribute_exists (self, key, _flow=_DOWN) :
        """
        attribute_exist (key)

        This method will check if the given key is known and was set explicitly.
        The call will also return 'True' if the value for that key is 'None'.
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return self._attributes_i_exists (us_key, _flow)


    ####################################
    def attribute_is_readonly (self, key, _flow=_DOWN) :
        """
        attribute_is_readonly (key)

        This method will check if the given key is readonly, i.e. cannot be
        'set'.  The call will also return 'True' if the attribute is final
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return self._attributes_i_is_readonly (us_key, _flow)


    ####################################
    def attribute_is_writeable (self, key, _flow=_DOWN) :
        """
        attribute_is_writeable (key)

        This method will check if the given key is writeable - i.e. not readonly.
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return self._attributes_i_is_writeable (us_key, _flow)


    ####################################
    def attribute_is_removable (self, key, _flow=_DOWN) :
        """
        attribute_is_writeable (key)

        This method will check if the given key can be removed.
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return self._attributes_i_is_removable (us_key, _flow)


    ####################################
    def attribute_is_vector (self, key, _flow=_DOWN) :
        """
        attribute_is_vector (key)

        This method will check if the given attribute has a vector value type.
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return self._attributes_i_is_vector (us_key, _flow)


    ####################################
    # fold the GFD.90 monitoring API into the attributes API
    ####################################
    def add_callback (self, key, cb, _flow=_DOWN) :
        """
        add_callback (key, cb)

        For any attribute change, the API will check if any callbacks are
        registered for that attribute.  If so, those callbacks will be called
        in order of registration.  This registration function will return an
        id (cookie) identifying the callback -- that id can be used to
        remove the callback.

        A callback is any callable python construct, and MUST accept three
        arguments::

            - STRING key: the name of the attribute which changed
            - ANY    val: the new value of the attribute
            - ANY    obj: the object on which this attribute interface was called

        The 'obj' can be any python object type, but is guaranteed to expose
        this attribute interface.

        The callback SHOULD return 'True' or 'False' -- on 'True', the callback
        will remain registered, and will thus be called again on the next
        attribute change.  On returning 'False', the callback will be
        unregistered, and will thus not be called again.  Returning nothing is
        interpreted as 'False', other return values lead to undefined behavior.

        Note that callbacks will not be called on 'Final' attributes (they will
        be called once as that attribute enters finality).
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return self._attributes_i_add_cb (us_key, cb, _flow)


    ####################################
    def remove_callback (self, key, id, _flow=_DOWN) :
        """
        remove_callback (key, id)

        This method allows to unregister a previously registered callback, by
        providing its id.  It is not an error to remove a non-existing cb, but
        a valid ID MUST be provided -- otherwise, a BadParameter is raised.

        If no ID is provided (id == None), all callbacks are removed for this
        attribute.
        """

        key    = self._attributes_t_keycheck   (key)
        us_key = self._attributes_t_underscore (key)
        return self._attributes_i_del_cb (us_key, id, _flow)



    ############################################################################
    #
    # Python property interface
    #
    # we assume that properties are always used in under_score notation.
    #
    ####################################
    def __getattr__ (self, key) :
        """ see L{get_attribute} (key) for details. """
        
        key  = self._attributes_t_keycheck (key)
        return self._attributes_i_get      (key, flow=self._DOWN)


    ####################################
    def __setattr__ (self, key, val) :
        """ see L{set_attribute} (key, val) for details. """

        key  = self._attributes_t_keycheck (key)
        return self._attributes_i_set      (key, val, flow=self._DOWN)


    ####################################
    def __delattr__ (self, key) :
        """ see L{remove_attribute} (key) for details. """
        
        key  = self._attributes_t_keycheck (key)
        return self._attributes_remove     (key, flow=self._DOWN)


################################################################################

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

