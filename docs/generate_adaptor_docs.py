
# --------------------------------------------------------------------
#
import re
import pprint
import saga.engine.registry


# --------------------------------------------------------------------
#
DOCROOT = "./source/adaptors"


# --------------------------------------------------------------------
#
def cleanup (text) :
    return re.sub ("\n\s*", "\n", text)


# --------------------------------------------------------------------
#
idx = "%s/%s.rst" % (DOCROOT, 'saga.adaptor.index')
i   = open (idx, 'w')

i.write ("#########\n")
i.write ("Adaptors:\n")
i.write ("#########\n")
i.write ("\n")
i.write (".. toctree::\n")
i.write ("   :numbered:\n")
i.write ("   :maxdepth: 1\n")
i.write ("\n")

for a in saga.engine.registry.adaptor_registry :

    m  = __import__ (a, fromlist=['Adaptor'])
    n  = m._ADAPTOR_NAME
    fn = "%s/%s.rst" % (DOCROOT, n)
    print "create %s" % fn
    i.write ("   %s\n" % n)

    details = "NO DETAILS KNOWN"
    version = "NO VERSION KNOWN"
    schemas = "NO SCHEMAS DOCUMENTED"
    classes = "NO API CLASSES DOCUMENTED"
    options = "NO OPTIONS SPECIFIED"
    cfgopts = [{
                'category'         : n,
                'name'             : 'enabled', 
                'type'             : bool, 
                'default'          : True, 
                'valid_options'    : [True, False],
                'documentation'    : "enable / disable %s adaptor"  % n,
                'env_variable'     : None
              }]

    
    if 'details' in m._ADAPTOR_DOC :
        details  =  m._ADAPTOR_DOC['details']
        details  =  cleanup (details)

    if 'version' in m._ADAPTOR_INFO :
        version  =  m._ADAPTOR_INFO['version']
        version  =  cleanup (version)

    if 'schemas' in m._ADAPTOR_DOC :
        schemas = ""
        for schema in m._ADAPTOR_DOC['schemas'] :
            text     = cleanup (m._ADAPTOR_DOC['schemas'][schema])
            schemas += "**%s** : %s\n" % (schema, text)

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

            options += "%s\n" % oname
            options += "%s\n" % ('-' * len(oname))
            options += "\n"
            options += "%s\n" % odoc
            options += "\n"
            options += "  - **type** : %s\n" % otype
            options += "  - **default** : %s\n" % odef
            options += "  - **environment** : %s\n" % oenv

            if len (oval) :
                options += "  - **valid options** : %s\n" % str(oval)

    if 'cpis' in m._ADAPTOR_INFO :
        classes      = ""
        classes_long = ""

        for cpi in m._ADAPTOR_INFO['cpis'] :
            classes      += "  - :class:`%s`\n" % cpi['type']
            classes_long += "\n"
            classes_long += "%s\n" % cpi['type']
            classes_long += "%s\n" % ('-' * len(cpi['type']))
            classes_long += "\n"
            classes_long += ".. autoclass:: %s.%s\n"  % (a, cpi['class'])
            classes_long += "   :members:\n"
          # classes_long += "   :undoc-members:\n"
            classes_long += "\n"


    f = open (fn, 'w')

    f.write ("\n")
    f.write ("%s\n" % ('*' * len(n)))
    f.write ("%s\n" % n)
    f.write ("%s\n" % ('*' * len(n)))
    f.write ("\n")
    f.write ("%s\n" % details)
    f.write ("\n")
    f.write ("Version\n")
    f.write ("=======\n")
    f.write ("\n")
    f.write ("%s\n" % version)
    f.write ("\n")
    f.write ("\n")
    f.write ("Supported Schemas\n")
    f.write ("=================\n")
    f.write ("\n")
    f.write ("%s\n" % schemas)
    f.write ("\n")
    f.write ("\n")
    f.write ("Configuration Options\n")
    f.write ("=====================\n")
    f.write ("\n")
    f.write ("%s\n" % options)
    f.write ("\n")
    f.write ("Supported API Classes\n")
    f.write ("=====================\n")
    f.write ("\n")
    f.write ("%s\n" % classes)
    f.write ("%s\n" % classes_long)
    f.write ("\n")

    f.close ()

i.write ("\n")
i.write ("\n")

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

