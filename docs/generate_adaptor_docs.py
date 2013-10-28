
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


__author__    = ["Andre Merzky", "Ole Weidner"]
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"

# --------------------------------------------------------------------
#
import re
import saga.engine.registry


# --------------------------------------------------------------------
#
DOCROOT = "./source/adaptors"


# --------------------------------------------------------------------
#
def cleanup (text) :
    """
    The text is unindented by the anmount of leading whitespace found in the
    first non-empty.  If that first line has no leading whitespace, the
    indentation of the second line is used.  This basically converts::

        string = '''some long
                    string with indentation which
                      has different
                      indentation levels
                    all over the string
                 '''

    to::

        some long
        string with indentation which
          has different
          indentation levels
        all over the string
    """
    l_idx = 1
    lines = text.split ('\n')

    # count leading non-empty lines
    for line in lines :
        if not line.strip () :
            l_idx += 1
        else :
            break

    # check if there is anything more to evaluate
    if len (lines) <= l_idx :
        return text

    # determine indentation of that line
    indent = 0
    for c in lines[l_idx] :
        if c == ' ' :
            indent += 1
        else : 
            break

    # if nothing found, check the following line
    if not indent :

        if len (lines) <= l_idx + 1:
            return text
        for c in lines[l_idx + 1] :
            if c == ' ' :
                indent += 1
            else : 
                break

    # if still nothing found, give up
    if not indent :
        return text


    # oitherwise trim all lines by that indentation
    out = ""
    replace = ' ' * indent
    for line in lines :
        out   += re.sub ("%s" % ' ' * indent, "", line)
        out   += "\n"

    return out


# --------------------------------------------------------------------
#
idx = "%s/%s.rst" % (DOCROOT, 'saga.adaptor.index')
i   = open (idx, 'w')

i.write (".. _chapter_adaptors:\n")
i.write ("\n")
i.write ("********\n")
i.write ("Adaptors\n")
i.write ("********\n")
i.write ("\n")
i.write (".. toctree::\n")
i.write ("   :numbered:\n")
i.write ("   :maxdepth: 1\n")
i.write ("\n")

for a in saga.engine.registry.adaptor_registry :

    m = None
    try :
        m  = __import__ (a, fromlist=['Adaptor'])
    except Exception as e:
        print "import from %s failed: %s" % (a, e)
        continue

    n  = m._ADAPTOR_NAME
    fn = "%s/%s.rst" % (DOCROOT, n)
    print "create %s" % fn
    i.write ("   %s\n" % n)

    description = "NO DESCRIPTION AVAILABLE"
    example     = "NO EXAMPLE AVAILABLE"
    version     = "NO VERSION KNOWN"
    schemas     = "NO SCHEMAS DOCUMENTED"
    classes     = "NO API CLASSES DOCUMENTED"
    options     = "NO OPTIONS SPECIFIED"
    cfgopts     = [{
                    'category'         : n,
                    'name'             : 'enabled', 
                    'type'             : bool, 
                    'default'          : True, 
                    'valid_options'    : [True, False],
                    'documentation'    : "enable / disable %s adaptor"  % n,
                    'env_variable'     : None
                  }]
    capable     = "NO CAPABILITIES SPECIFIED"
    capabs      = []

    
    if 'description' in m._ADAPTOR_DOC :
        description  =  m._ADAPTOR_DOC['description']
        description  =  cleanup(description)

    print m._ADAPTOR_INFO['name']
    print m._ADAPTOR_DOC.keys ()

    if 'example' in m._ADAPTOR_DOC :
        example = m._ADAPTOR_DOC['example']

    if 'version' in m._ADAPTOR_INFO :
        version  =  m._ADAPTOR_INFO['version']
        version  =  cleanup (version)

    if 'schemas' in m._ADAPTOR_DOC :

        schemas  = "%s %s\n"       % ('='*24, '='*60)
        schemas += "%-24s %-60s\n" % ('schema', 'description')
        schemas += "%s %s\n"       % ('='*24, '='*60)

        for schema in sorted(m._ADAPTOR_DOC['schemas'], key=len, reverse=False):
            text     = cleanup (m._ADAPTOR_DOC['schemas'][schema])
            schemas += "%-24s %-60s\n" % ("**"+schema+"://**", text)

        schemas += "%s %s\n"       % ('='*24, '='*60)


    if 'cfg_options' in m._ADAPTOR_DOC :
        cfgopts += m._ADAPTOR_DOC['cfg_options']
        options  = ""

        for o in cfgopts :
            oname = o['name']
            otype = o['type']
            odef  = o['default']
            odoc  = cleanup (o['documentation'])
            oenv  = o['env_variable']
            oval  = []

            if 'valid_options' in o :
                oval  = o['valid_options']

            options += "%s\n%s\n" % (oname, "*"*len(oname))
            options += "\n"
            options += "%s\n" % odoc
            options += "\n"
            options += "  - **type** : %s\n" % otype
            options += "  - **default** : %s\n" % odef
            options += "  - **environment** : %s\n" % oenv

            if len (oval) :
                options += "  - **valid options** : %s\n" % str(oval)

            options += "\n"


    if 'capabilities' in m._ADAPTOR_DOC :
        capabs   = m._ADAPTOR_DOC['capabilities']
        capable  = ""

        cap_headers = {
            'jdes_attributes': 'Supported Job Description Attributes' ,
            'job_attributes' : 'Supported Job Attributes' ,
            'metrics'        : 'Supported Monitorable Metrics' ,
            'contexts'       : 'Supported Context Types' ,
            'ctx_attributes' : 'Supported Context Attributes' 
        }

        for cname in capabs  :

            header   = cname
            if cname in cap_headers :
                header = cap_headers[cname]

            capable += "%s\n%s\n" % (header, "*"*len(header))
            capable += "\n"

            capab = capabs[cname]


            if type(capab) == list :
                for key in capab :
                    capable += "  - %s\n" % key
            elif type(capab) == dict :
            
                capable += "%s %s\n"       % ('='*60, '='*60)
                capable += "%60s %s\n"     % ('Attribute', 'Description')
                capable += "%s %s\n"       % ('='*60, '='*60)
                for key in capab :
                    val = capab[key]
                  # capable += "  - *%s*: %s\n" % (key,val)
                    capable += "%60s %s\n" % (':ref:`security_contexts` : ' + key, val)
                capable += "%s %s\n"       % ('='*60, '='*60)

            capable += "\n"


    if 'cpis' in m._ADAPTOR_INFO :
        classes      = ""
        classes_long = ""

        is_context = True
        for cpi in m._ADAPTOR_INFO['cpis'] :

            if cpi['type'] != 'saga.Context' :
                is_context = False

            if is_context :
                # do not auto-document context adaptors -- those are done manually
                print "skip   %s (context)" % fn
                continue
            else:
                classes      += "  - :class:`%s`\n" % cpi['type']
                classes_long += "\n"
                classes_long += "%s\n" % cpi['type']
                classes_long += "%s\n" % ('*' * len(cpi['type']))
                classes_long += "\n"
                classes_long += ".. autoclass:: %s.%s\n"  % (a, cpi['class'])
                classes_long += "   :members:\n"
              # classes_long += "   :undoc-members:\n"
                classes_long += "\n"



    f = open (fn, 'w')

    f.write ("\n")
    f.write ("%s\n" % ('#' * len(n)))
    f.write ("%s\n" % n)
    f.write ("%s\n" % ('#' * len(n)))
    f.write ("\n")
    f.write ("Description\n")
    f.write ("-----------\n")
    f.write ("%s\n" % description)
    f.write ("\n")
    f.write ("\n")

    if not is_context :
        f.write ("Supported Schemas\n")
        f.write ("-----------------\n")
        f.write ("\nThis adaptor is triggered by the following URL schemes:\n\n")
        f.write ("%s\n" % schemas)
        f.write ("\n")
        f.write ("\n")

    f.write ("Example\n")
    f.write ("-------\n")
    f.write ("\n")
    if example == "NO EXAMPLE AVAILABLE":
        f.write("%s\n" % example)
    else:
        f.write (".. literalinclude:: ../../../%s\n" % example)
    f.write ("\n")
    f.write ("\n")

    if not is_context :
        f.write ("Configuration Options\n")
        f.write ("---------------------\n")
        f.write ("Configuration options can be used to control the adaptor's \
runtime behavior. Most adaptors don't need any configuration options \
to be set in order to work. They are mostly for controlling experimental \
features and properties of the adaptors.\n\n \
.. seealso:: More information about configuration options can be found in \
the :ref:`conf_file` section.\n")
        f.write ("\n")
        f.write ("%s\n" % options)
        f.write ("\n")

    f.write ("Capabilities\n")
    f.write ("------------\n")
    f.write ("\n")
    f.write ("%s\n" % capable)
    f.write ("\n")

    if not is_context :
        f.write ("Supported API Classes\n")
        f.write ("---------------------\n")
        f.write ("\nThis adaptor supports the following API classes:\n")
        f.write ("%s\n" % classes)
        f.write ("Method implementation details are listed below.\n")
        f.write ("%s\n" % classes_long)
        f.write ("\n")

    f.close ()

i.write ("\n")
i.write ("\n")



