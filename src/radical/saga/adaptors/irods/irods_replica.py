
__author__    = "Andre Merzky, Ashley Z, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


# TODO: Create function to check for all the iRODS error codes and give
#       a better error readout?

# TODO: Add debug output for -all- icommands showing how they are executed
#       IE "[DEBUG] Executing: ils -l /mydir/"

# TODO: Implement resource package and use it to grab resource information

# TODO: Implement Andre's changes in the iRODS adaptors!


"""iRODS replica adaptor implementation 
   Uses icommands commandline tools
"""

__author__    = "Ashley Zebrowski"
__copyright__ = "Copyright 2012-2013, Ashley Zebrowski"
__license__   = "MIT"

import os
import shutil
import string

import radical.saga as rs

SYNC_CALL  = rs.adaptors.cpi.decorators.SYNC_CALL
ASYNC_CALL = rs.adaptors.cpi.decorators.ASYNC_CALL


###############################################################################
# adaptor info
#

_ADAPTOR_NAME          = 'radical.saga.adaptor.replica.irods'
_ADAPTOR_SCHEMAS       = ['irods']
_ADAPTOR_OPTIONS       = []
_ADAPTOR_CAPABILITIES  = {}
_ADAPTOR_DOC           = {
    'name'             : _ADAPTOR_NAME,
    'cfg_options'      : _ADAPTOR_OPTIONS, 
    'capabilities'     : _ADAPTOR_CAPABILITIES,
    'description'      : 'The iRODS replica adaptor.',
    'details'          : """This adaptor interacts with the iRODS data
                            management system, by using the iRODS command line
                            tools.""",
    "example"          : "examples/replica/irods/irods_test.py",
    'schemas'          : {'irods'  : 'irods schema'
    },
}

_ADAPTOR_INFO          = {
    'name'             : _ADAPTOR_NAME,
    'version'          : 'v0.1',
    'schemas'          : _ADAPTOR_SCHEMAS,
    'cpis'             : [{
                              'type' : 'radical.saga.replica.LogicalDirectory',
                              'class': 'IRODSDirectory'
                          }, 
                          {
                              'type' : 'radical.saga.replica.LogicalFile',
                              'class': 'IRODSFile'
                          }
                         ]
}


# ------------------------------------------------------------------------------
#
class irods_logical_entry:

    '''class to hold info on an iRODS logical file or directory
    '''
    def __init__(self):
        self.name = "undefined"
        self.locations = []
        self.size = 1234567899
        self.owner = "undefined"
        self.date = "1/1/1111"
        self.is_directory = False


    def __str__(self):
        return ' '.join([str(self.name),
                         "/".join(self.locations),
                         str(self.size),
                         str(self.owner),
                         str(self.date),
                         str(self.is_directory)])


        # ------------------------------------------------------------------------------
        #
class irods_resource_entry:
    '''class to hold info on an iRODS resource 
    '''
    # Resources (not groups) as retreived from ilsresc -l look like this:
    #
    # resource name:     BNL_ATLAS_2_FTP
    # resc id:           16214
    # zone:              osg
    # type:              MSS universal driver
    # class:             compound
    # location:          gw014k1.fnal.gov
    # vault:             /data/cache/BNL_ATLAS_2_FTPplaceholder
    # free space:
    # status:            up
    # info:
    # comment:
    # create time:       01343055975: 2012-07-23.09:06:15
    # modify time:       01347480717: 2012-09-12.14:11:57
    # ----

    # Resource groups look like this (shortened):
    #
    # resource group:    osgGridFtpGroup
    # Includes resource: NWICG_NotreDame_FTP
    # Includes resource: UCSDT2-B_FTP
    # Includes resource: UFlorida-SSERCA_FTP
    # Includes resource: cinvestav_FTP
    # Includes resource: SPRACE_FTP
    # Includes resource: NYSGRID_CORNELL_NYS1_FTP
    # Includes resource: Nebraska_FTP
    # -----

    # ----------------------------------------------------------------
    #
    #
    def __init__ (self):
        # are we a resource group? 
        self.is_resource_group = False

        # individual resource-specific properties
        self.name              = None
        self.zone              = None
        self.type              = None
        self.resource_class    = None
        self.location          = None
        self.vault             = None
        self.free_space        = None
        self.status            = None
        self.info              = None
        self.comment           = None
        self.create_time       = None
        self.modify_time       = None

        # resource group-specific properties
        self.group_members     = []


###############################################################################
# The adaptor class

class Adaptor (rs.adaptors.base.Base):
    """ 
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    """

    # ----------------------------------------------------------------
    #
    #
    def __init__ (self) :
        rs.adaptors.base.Base.__init__ (self, 
                                          _ADAPTOR_INFO,
                                          _ADAPTOR_OPTIONS)


    def sanity_check (self) :
        try:
            # temporarily silence logger
            lvl = self._logger.getEffectiveLevel ()
            self._logger.setLevel (rs.utils.logger.ERROR)

            # open a temporary shell
            self.shell = rs.utils.pty_shell.PTYShell(
                             rs.url.Url("ssh://localhost"), logger=self._logger)

            # run ils, see if we get any errors -- if so, fail the
            # sanity check
            rc, _, _ = self.shell.run_sync("ils")
            if rc != 0:
                raise Exception("sanity check error")

        except Exception as ex:
            raise rs.NoSuccess ("Could not run iRODS/ils.  Check iRODS"
                                  "environment and certificates (%s)" % ex)
        finally :
            # re-enable logger
            self._logger.setLevel (lvl)

        # try ienv or imiscsvrinfo later? ( check for error messages )



    # ----------------------------------------------------------------
    #
    #
    def irods_get_directory_listing (self, irods_dir, wrapper) :
        '''Function takes an iRODS logical directory as an argument,
           and returns a list of irods_logical_entry instances containing
           information on files/directories found in the directory argument.
           It uses the commandline tool ils.
           :param self: reference to adaptor which called this function
           :param dir: iRODS directory we want to get a listing of
           :param wrapper: the wrapper we will make our iRODS
           commands with
        '''

        result = []
        try:
            cw = wrapper

            # execute the ils -L command
            ret, out, _ = cw.run_sync ("ils -L %s" % irods_dir)

            # make sure we ran ok
            if ret != 0:
                raise rs.NoSuccess("Could not open directory %s [%s]: %s"
                                  % (irods_dir, str(ret), out))

            # strip extra linebreaks from stdout, make a list from the
            # linebreaks that remain, skip first entry which just tells us the
            # current directory
            for item in out.strip().split("\n"):

                # if we are listing a directory or remote resource file
                # location i.e.
                # 
                # [azebro1@gw68]$ ils -L /osg/home/azebro1
                # /osg/home/azebro1:
                #    azebro1           1 UFlorida-SSERCA_FTP            12 2012-11-14.09:55 & irods-test.txt
                #          /data/cache/UFlorida-SSERCA_FTPplaceholder/home/azebro1/irods-test.txt    osgGridFtpGroup
                #
                # then we want to ignore that entry (not using it for now)
                if item.strip().startswith("/"):
                    continue 

                # remove whitespace
                item = item.strip()

                # entry for file or directory
                dir_entry = irods_logical_entry()

                # if we have a directory here
                if item.startswith("C- "):
                    dir_entry.name = item[3:]
                    dir_entry.is_directory = True

                # if we have a file here
                else:
                    # ils -L output looks like this after you split it:
                    #  0           1    2                      3     4                   5    6
                    # ['azebro1', '1', 'UFlorida-SSERCA_FTP', '12', '2012-11-14.09:55', '&', 'irods-test.txt']
                    # not sure what 1 and 5 are ... 
                    dir_entry.owner = item.split()[0]
                    dir_entry.locations = [item.split()[2]]
                    dir_entry.size = item.split()[3]
                    dir_entry.date = item.split()[4]
                    dir_entry.name = item.split()[6]

                result.append(dir_entry)

            # merge all entries on the list with duplicate filenames into a
            # single entry with one filename and multiple resource locations
            final_list = []
            for item in result:
                if item.name in [i.name for i in final_list]:
                    # duplicate name, merge this entry with the previous one
                    for final_list_item in final_list:
                        if final_list_item.name == item.name:
                            final_list_item.locations.append(item.locations[0])
                else:
                    final_list.append(item)
            return final_list

        except Exception as e:
            raise rs.NoSuccess ("Couldn't get directory listing: %s %s " % e)

        return result


    # ----------------------------------------------------------------
    #
    #
    def irods_get_resource_listing(self):
        ''' Return a list of irods resources and resource groups with
            information stored in irods_resource_entry format.
            It uses commandline tool ilsresc -l 
            :param self: reference to adaptor which called this function
        '''
        result = []
        try:
            # execute the ilsresc -l command
            ret, out, _ = self.shell.run_sync("ilsresc -l")

            # make sure we ran ok
            if ret != 0:
                raise Exception("Could not obtain resources with ilsresc -l")

            # convert our command's stdout to a list of text lines
            cw_result_list = out.strip().split("\n")

            # list of resource entries we will save our results to
            result = []

            # while loop instead of for loop so we can mutate the list
            # as we iterate
            while cw_result_list:
                entry = irods_resource_entry()

                # get our next line from the FRONT of the list
                line = cw_result_list.pop(0)

                # check to see if this is the beginning of a
                # singular resource entry 
                if line.startswith("resource name: "):
                    # singular resource entry output from ilsresc -l
                    # LINE NUMBERS AND PADDING ADDED
                    # ex. actual output, line 0 starts like "resource name"
                    # 0  resource name: BNL_ATLAS_2_FTP
                    # 1  resc id: 16214
                    # 2  zone: osg
                    # 3  type: MSS universal driver
                    # 4  class: compound
                    # 5  location: gw014k1.fnal.gov
                    # 6  vault: /data/cache/BNL_ATLAS_2_FTPplaceholder
                    # 7  free space:
                    # 8  status: up
                    # 9  info:
                    # 10 comment:
                    # 11 create time: 01343055975: 2012-07-23.09:06:15
                    # 12 modify time: 01347480717: 2012-09-12.14:11:57
                    # 13 ----
                    entry.name = line[len("resource name: "):].strip()
                    entry.is_resource_group = False

                    # TODO: SAVE ALL THE OTHER INFO
                    for i in range(13):
                        cw_result_list.pop(0)

                    # add our resource to the list
                    result.append(entry)

                # check to see if this is an entry for a resource group
                elif line.startswith("resource group: "):
                    entry.name = line[len("resource group: "):].strip()
                    entry.is_resource_group = True

                    # continue processing ilsresc -l results until we
                    # are at the end of the resource group information
                    # ----- is not printed if there are no further entries
                    # so we have to make sure to check we don't pop off
                    # an empty stack too
                    #
                    # TODO: SAVE THE LIST OF RESOURCES IN A RESOURCE GROUP
                    while cw_result_list and not line.startswith("-----"):
                        line = cw_result_list.pop(0)

                    result.append(entry)

                # for some reason, we're at a line which we have 
                # no idea how to handle this is bad -- throw an error
                else:
                    raise rs.NoSuccess("Error parsing iRODS ilsresc -l output")

            return result

        except Exception as e:
            raise rs.NoSuccess ("Couldn't get resource listing: %s " % (str(e)))


###############################################################################
#
# logical_directory adaptor class
#
class IRODSDirectory (rs.adaptors.cpi.replica.LogicalDirectory) :

    # ----------------------------------------------------------------
    #
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (IRODSDirectory, self)
        self._cpi_base.__init__ (api, adaptor)

        self.name  = None
        self.size  = None
        self.owner = None
        self.date  = None

        self.shell = rs.utils.pty_shell.PTYShell (rs.url.Url("ssh://localhost"))

    def __del__ (self):
        self._logger.debug("Deconstructor for iRODS directory")
        self.shell.close()

    # ----------------------------------------------------------------
    #
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, url, flags, session) :

        self._url     = url
        self._flags   = flags
        self._session = session

        self._init_check ()

        return self


    # ----------------------------------------------------------------
    #
    #
    @ASYNC_CALL
    def init_instance_async (self, adaptor_state, url, flags, session, ttype) :

        self._url     = url
        self._flags   = flags
        self._session = session
        self._init_check ()

        t = rs.task.Task ()
        t._set_result (rs.replica.LogicalDirectory(
                url, flags, session, _adaptor_name=_ADAPTOR_NAME))
        t._set_state  (rs.task.DONE)

        return t


    # ----------------------------------------------------------------
    #
    #
    def _init_check (self) :

        if self._url.host != "localhost":
            raise rs.BadParameter._log(self._logger, "iRODS adaptor does NOT"
                                         " currently support accessing remote"
                                         " hosts via URLs.  To access the"
                                         " iRODS environment currently"
                                         " established on the local"
                                         " machine, please use localhost"
                                         " as your hostname.")

    # ----------------------------------------------------------------
    #
    #
    @SYNC_CALL
    def get_url (self) :
        return self._url


    # ----------------------------------------------------------------
    #
    #
    @SYNC_CALL
    def open (self, url, flags) :

        if not url.scheme and not url.host : 
            url = rs.url.Url (str(self._url) + '/' + str(url))

        f = rs.replica.LogicalFile (url, flags, self._session,
                                      _adaptor_name=_ADAPTOR_NAME)
        return f


    # ----------------------------------------------------------------
    #
    #
    @SYNC_CALL
    def make_dir (self, path, flags) :

        # complete_path = dir_obj._url.path
        complete_path = rs.Url(path).get_path()

        # attempt to run iRODS mkdir command
        try:
            ret, out, _ = self.shell.run_sync("imkdir %s" % complete_path)

            if ret != 0:
                raise rs.NoSuccess("Could not create dir %s, [%s]: %s"
                                  % (complete_path, str(ret), out))

        except Exception as ex:
            # did the directory already exist?
            if "CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME" in str(ex):
                raise rs.AlreadyExists ("Directory already exists.")

            # couldn't create for unspecificed reason
            raise rs.NoSuccess ("Couldn't create directory. %s" % ex)

        return


    # ----------------------------------------------------------------
    #
    #
    @SYNC_CALL
    def remove (self, path, flags) :
        '''This method is called upon logicaldir.remove() '''

        complete_path = rs.Url(path).get_path()

        try:
            self._logger.debug("Executing: irm -r %s" % complete_path)
            ret, out, _ = self.shell.run_sync("irm -r %s" % complete_path)

            if ret != 0:
                raise rs.NoSuccess("Could not remove directory %s [%s]: %s"
                                  % (complete_path, str(ret), out))

        except Exception as ex:

            # was there no directory to delete?
            if "does not exist" in str(ex):
                raise rs.DoesNotExist("Directory %s does not exist."
                                     % complete_path)

            # couldn't delete for unspecificed reason
            raise rs.NoSuccess("Couldn't delete directory %s because %s"
                              % (complete_path, ex))

        return


    # ----------------------------------------------------------------
    #
    #
    @SYNC_CALL
    def list (self, npat, flags) :

       # TODO: Make this use the irods_get_directory_listing

        complete_path = self._url.path
        result = []

        self._logger.debug("Attempting to get directory listing for logical"
                           "path %s" % complete_path)

        try:
            ret, out, _ = self.shell.run_sync("ils %s" % complete_path)

            if ret != 0:
                raise Exception("Could not open directory")

            # strip extra linebreaks from stdout, make a list w/ linebreaks,
            # skip first entry which tells us the current directory
            for item in out.strip().split("\n")[1:]:
                item = item.strip()
                if item.startswith("C- "):
                    # result.append("dir " + item[3:])
                    result.append(item[3:])
                else:
                    # result.append("file " + item)
                    result.append(item)

        except Exception as ex:
            raise rs.NoSuccess ("Couldn't list directory: %s " % (str(ex)))

        return result


######################################################################
#
# logical_file adaptor class
#
class IRODSFile (rs.adaptors.cpi.replica.LogicalFile) :

    # ----------------------------------------------------------------
    #
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (IRODSFile, self)
        self._cpi_base.__init__ (api, adaptor)

        self.name         = None
        self.locations    = []
        self.size         = None
        self.owner        = None
        self.date         = None
        self.is_directory = False

        self.shell = rs.utils.pty_shell.PTYShell (rs.url.Url("ssh://localhost"))

        # TODO: "stat" the file

    def __del__ (self):
        self._logger.debug("Deconstructor for iRODS file")
        self.shell.close()


    # ----------------------------------------------------------------
    #
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, url, flags, session) :

        self._url     = url
        self._flags   = flags
        self._session = session

        self._init_check ()

        return self


    # ----------------------------------------------------------------
    #
    #
    @ASYNC_CALL
    def init_instance_async (self, adaptor_state, url, flags, session, ttype) :

        self._url     = url
        self._flags   = flags
        self._session = session

        self._init_check ()

        t = rs.task.Task ()

        t._set_result (rs.replica.LogicalFile (url, flags, session,
                                                 _adaptor_name=_ADAPTOR_NAME))
        t._set_state  (rs.task.DONE)

        return t


    # ----------------------------------------------------------------
    #
    #
    def _init_check (self) :
        url   = self._url

        if url.port :
            raise rs.BadParameter("Cannot handle url %s (has fragment)" % url)
        if url.query :
            raise rs.BadParameter("Cannot handle url %s (has query)"    % url)
        if url.username :
            raise rs.BadParameter("Cannot handle url %s (has username)" % url)
        if url.password :
            raise rs.BadParameter("Cannot handle url %s (has password)" % url)

        self._path = url.path


    # ----------------------------------------------------------------
    #
    #
    @SYNC_CALL
    def get_url (self) :
        return self._url

    # ----------------------------------------------------------------
    #
    #
    @ASYNC_CALL
    def get_url_async (self, ttype) :

        t = rs.task.Task ()

        t._set_state  = rs.task.DONE
        t._set_result = self._url

        return t


    # ----------------------------------------------------------------
    #
    #
    @SYNC_CALL
    def copy_self (self, target, flags) :

        tgt_url = rs.url.Url (target)
        tgt     = tgt_url.path
        src     = self._url.path

        if tgt_url.schema :
            if not tgt_url.schema.lower() in _ADAPTOR_SCHEMAS:
                raise rs.BadParameter("Cannot handle schema for %s" % target)

        if tgt[0] != '/' :
            tgt = "%s/%s"   % (os.path.dirname (src), tgt)

        print(" copy %s %s" % (self._url, tgt))
        shutil.copy2 (src, tgt)


    # ----------------------------------------------------------------
    #
    #
    @SYNC_CALL
    def list_locations(self):
        '''
        This method is called upon logicaldir.list_locations()
        '''

        # return a list of all replica locations for a file
        path    = self._url.get_path()
        listing = self._adaptor.irods_get_directory_listing(path, self.shell)

        return listing[0].locations


    # ----------------------------------------------------------------
    #
    #
    @SYNC_CALL
    def get_size_self (self) :
        '''
        This method is called upon logicaldir.get_size()
        '''

        return self.size


    # ----------------------------------------------------------------
    #
    #
    @SYNC_CALL
    def remove_location(self, location):
        '''This method is called upon logicaldir.remove_locations()
        '''     
        raise rs.NotImplemented._log (self._logger, "Not implemented")
        return


    # ----------------------------------------------------------------
    #
    #
    @SYNC_CALL
    def replicate (self, target, flags):
        '''This method is called upon logicaldir.replicate()
        '''        

        # path to file we are replicating on iRODS
        complete_path = self._url.get_path()        

        # TODO: Verify Correctness in the way the resource is grabbed
        query = rs.Url(target).get_query()
        resource = query.split("=")[1]
        self._logger.debug("Attempting to replicate logical file %s to "
                           "resource/resource group %s" 
                           % (complete_path, resource))

        try:
            self._logger.debug("Executing: irepl -R %s %s"
                              % (resource, complete_path))
            ret, out, _ = self.shell.run_sync("irepl -R %s %s" 
                                             % (resource, complete_path))

            if ret:
                raise Exception("Could not replicate logical file %s to"
                                "resource/resource group %s, errorcode %s: %s"
                               % (complete_path, resource, str(ret), out))

        except Exception as ex:
            raise rs.NoSuccess._log(self._logger, "replicate failed: %s" % ex)


    # ----------------------------------------------------------------
    #
    # TODO: This is COMPLETELY untested, as it is unsupported on the only iRODS
    # machine I have access to.
    #
    @SYNC_CALL
    def move_self (self, target, flags) :
        '''This method is called upon logicaldir.move() '''

        # path to file we are moving on iRODS
        source_path = self._url.get_path()
        dest_path   = rs.Url(target).get_path()

        self._logger.debug("Attempting to move logical file %s to location %s"
                          % (source_path, dest_path))

        try:
            ret, out, _ = self.shell.run_sync("imv %s %s"
                                             % (source_path, dest_path))

            if ret:
                raise rs.NoSuccess("Could not move logical file %s to location"
                                   "%s, errorcode %s: %s" 
                                  % (source_path, dest_path, str(ret), out))

        except Exception as ex:
            raise rs.NoSuccess ("Couldn't move file: %s" % ex)


    # ----------------------------------------------------------------
    #
    #
    def add_location (self, name, ttype) :
        '''This method is called upon logicaldir.add_location() '''

        # TODO: REMOVE UPLOAD CALL/MERGE INTO HERE IF SUPERFLUOUS
        self.upload(name, None, None)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def remove_self (self, flags) :
        '''This method is called upon logicalfile.remove() '''

        complete_path = self._url.get_path()
        self._logger.debug("Attempting to remove file at: %s" % complete_path)

        try:
            ret, out, _ = self.shell.run_sync("irm %s" % complete_path)

            if ret:
                raise rs.NoSuccess("Could not remove file %s [%s]: %s"
                                  % (complete_path, str(ret), out))

        except Exception as ex:
            # couldn't delete for unspecificed reason
            raise rs.NoSuccess("delete %s failed: %s" % (complete_path, ex))

        return


    ######################################################################
    #
    #
    # From a convo with Andre M...
    #
    # So, if you want to have a logical file in that logical dir, you would
    # create it:
    #
    #   myfile = mydir.open(irods.tar.gz, rs.replica.Create | \
    #                                     rs.replica.ReadWrite)
    # and then upload
    #
    #   myfile.upload ("file://home/ashley/my/local/filesystem/irods.tar.gz")
    #
    # OR (revised)
    #
    #   myfile.upload("file://home/ashley/my/local/filesystem/irods.tar.gz",
    #                 "irods://.../?resource=host3")
    # 
    @SYNC_CALL
    def upload (self, source, target, flags) :
        '''Uploads a file from the LOCAL, PHYSICAL filesystem to
           the replica management system.
           @param source: URL (should be file:// or local path) of local file
           @param target: Optional param containing ?resource=myresource query
                          This will upload the file to a specified iRODS
                          resource or group.
        '''

        # TODO: Make sure that the source URL is a local/file:// URL
        complete_path = rs.Url(source).get_path()

        # extract the path from the LogicalFile object, excluding
        # the filename
        destination_path = self._url.get_path()[0:string.rfind(
                           self._url.get_path(), "/") + 1]

        try:
            # note we're uploading
            self._logger.debug("Beginning upload operation "
                               "will register file in logical dir: %s"
                              % destination_path)

            # the query holds our target resource
            query = rs.Url(target).get_query()

            # list of args we will generate
            arg_list = ""

            # parse flags + generate args
            if flags:
                if flags & rs.namespace.OVERWRITE:
                    arg_list += "-f "

            # was no resource selected?
            if not query:
                self._logger.debug("Attempting to upload to default resource")
                ret, out, _ = self.shell.run_sync("iput %s %s %s" %
                                    (arg_list, complete_path, destination_path))

            # resource was selected, have to parse it and supply to iput -R
            else:
                # TODO: Verify correctness
                resource = query.split("=")[1]

                self._logger.debug("upload to %s" % resource)
                ret, out, _ = self.shell.run_sync("iput -R %s %s %s %s" %
                                         (resource, arg_list, complete_path,
                                          destination_path))

            if ret:
                raise rs.NoSuccess("Could not upload file %s, errorcode %s: %s"
                                  % (complete_path, str(ret), out))

        except Exception as ex:
            # couldn't upload for unspecificed reason
            raise rs.NoSuccess._log (self._logger, "upload failed: %s" % ex)

        return


    @SYNC_CALL
    def download (self, name, source, flags) :
        '''Downloads a file from the REMOTE REPLICA FILESYSTEM to a local
           directory.
           @param target: param containing a local path/filename
                          to save the file to
           @param source: Optional param containing a remote replica to retrieve
                          the file from (not evaluated, yet)
        '''

        target = name

        # TODO: Make sure that the target URL is a local/file:// URL
        # extract the path from the LogicalFile object, excluding
        # the filename
        logical_path = self._url.get_path()

        # fill in our local path if one was specified
        local_path = ""
        if target:
            local_path = rs.Url(target).get_path()

        try:
            # var to hold our command result, placed here to keep in scope
            ret = out = _ = 0

            # mark that this is experimental/may not be part of official API
            self._logger.debug("Beginning download operation "
                               "will download logical file: %s, "
                               "specified local target is %s"
                              % (logical_path, target))

            if target: cmd = "iget %s %s" % (logical_path, local_path) 
            else     : cmd = "iget %s"    %  logical_path

            self._logger.debug("Executing: %s" % cmd)
            ret, out, _ = self.shell.run_sync(cmd)

            # check our result
            if ret:
                raise rs.NoSuccess("download failed: %s [%s]: %s"
                                  % (logical_path, str(ret), out))

        except Exception as ex:
            # couldn't download for unspecificed reason
            raise rs.NoSuccess ("Couldn't download file. %s" % ex)


# ------------------------------------------------------------------------------

