# -*- coding: utf-8 -*-
'''A incomplete clone of the ftplib that wraps the psftp client included with putty to provide sftp support.
Putty can be downloaded from: http://www.putty.org/
'''
import ftplib
import os
import time
import socket
import subprocess
import stat
import re
import sys

PROCESS_TIMEOUT_SEC = 10
PSFTP_PATH = '''/usr/bin/sftp'''   
        
def timeout(func, args=(), kwargs={}, timeout_duration=1, default=None):
    '''This function will spwan a thread and 
    return the given default value if the timeout_duration is exceeded or the function all raises an exception.
    ''' 
    import threading
    class InterruptableThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
            self.result = default
        def run(self):
            try:
                self.result = func(*args, **kwargs)
            except:
                self.result = default
    it = InterruptableThread()
    it.start()
    it.join(timeout_duration)
    if it.isAlive():
        return it.result
    else:
        return it.result        


class SFTP:
    '''A subprocess wrapper for the psftp tool for encrypted data transfer'''
    connected=False
    
    def __init__(self):
        self.sftp_path = PSFTP_PATH
        self.proc = None
        self.min_dl_rate_kbps = 15
        self.endline = '\r\n'
        self.welcome = ''
        
        
    def _wait_for_text(self, read_until='login as', timeout_sec=PROCESS_TIMEOUT_SEC):
        '''Function reads the output from self.proc via stdout.read().
        This function is blocking and should be run in a separate thread.
        
        In this program the "timeout" function is used to launch
        this function in a separate thread with a timeout.
        
        read_until == 
        timeout_sec == 
        
        NOTE: this function does have a timeout value, but will only function
        if the read() function has continuous input.  read() waiting for new character will BLOCK!
        '''
        resp = ''
        cur_time = time.time()
        duration = 0
        while read_until not in resp or duration > timeout_sec :
            letter = self.proc.stdout.read(1)
            resp += letter
            
            prev_time = cur_time
            cur_time = time.time()
            
            duration = cur_time - prev_time
        self.proc.stdout.flush()
        return resp    
    
    
    def _clean_path(self, path):
        '''Normalize and convert to unix format'''
        if path:
            path = os.path.normpath(path)
            path = path.replace('\\','/')
            
        return path        
               
    
        
    def set_debuglevel(self, level=0):
        '''Set the instance’s debugging level. This controls the amount of debugging output printed. 
        The default, 0, produces no debugging output. 
        A value of 1 produces a moderate amount of debugging output, generally a single line per request. 
        A value of 2 or higher produces the maximum amount of debugging output, 
        logging each line sent and received on the control connection.
        '''
        raise CommandNotImplemented()
    
    
    def connect(self, server=None):
        '''Connect to remote server via the psftp program'''
        
        if os.path.exists(self.sftp_path):
            proc_args = (self.sftp_path, server)
            
            try:
                self.proc = subprocess.Popen(proc_args, 
                                             bufsize=1, 
                                             shell=True, 
                                             stdin=subprocess.PIPE, 
                                             stdout=subprocess.PIPE, 
                                             stderr=subprocess.PIPE)
                
            except:
                raise socket.gaierror('unable to open process to (%s)' % self.sftp_path)
        else:
            msg = 'Invalid Path to sftp tool: %s\n' % (self.sftp_path)
            msg += '\tThis wrapper uses the Putty psftp tool for secure file transfer.\n'
            msg += '\tPutty can be downloaded from: http://www.putty.org/\n' % ()
            msg += '\tPSFTP_PATH needs to be set to the proper path for the installed psftp.exe file\n'
            
            raise IOError(msg)
        
        
    def getwelcome(self):
        '''Return the welcome message sent by the server in reply to the initial connection. 
        (This message sometimes contains disclaimers or help information that may be relevant to the user.)
        '''
        return self.welcome
         
    
    def login(self, username=None, password=None):
        '''Login to given server via psftp
        If login proceeds without exception login is considered a success
        '''
        username_prompt = 'login as:'
        password_prompt = 'password:'
        cmd_prompt = 'sftp>'
        
        # Wait for the username login prompt
        # --> use timeout on wait4prompt function
        prompt_found = timeout(self._wait_for_text,args=(username_prompt,), timeout_duration=PROCESS_TIMEOUT_SEC, default=False )
        
        if prompt_found:
            username_input = username + self.endline
            self.proc.stdin.write(username_input)
            self.proc.stdin.flush()
            
            # Wait for the username login prompt
            # --> use timeout on wait4prompt function
            prompt_found = timeout(self._wait_for_text,args=(password_prompt,), timeout_duration=PROCESS_TIMEOUT_SEC, default=False )
            
            if prompt_found:
                self.welcome = prompt_found
                password_input = password + self.endline
                self.proc.stdin.write(password_input)
                self.proc.stdin.flush()
                
                # Wait for standard command prompt
                # --> If timeout assume invalid username/password and raise ftplib.error_perm
                # --> use timeout on wait4prompt function
                prompt_found = timeout(self._wait_for_text,args=(cmd_prompt,), timeout_duration=PROCESS_TIMEOUT_SEC, default=False )
                
                if not prompt_found:
                    self.kill()
                    self.proc.wait()
                    raise ftplib.error_perm('Invalid Username/Password: process timedout (prompt: %s)' % (cmd_prompt))
            else:
                self.kill()
                self.proc.wait()
                raise ProcessTimeout('Expected Prompt not found, process timedout (prompt: %s)' % (password_prompt)) 
        else:
            self.kill()
            self.proc.wait()
            raise ProcessTimeout('Expected Prompt not found, process timedout (prompt: %s)' % (username_prompt))
        
        
    def abort(self):
        '''Abort a file transfer that is in progress.'''
        raise CommandNotImplemented()
    
    
    def sendcmd(self, command):
        '''Send a simple command string to the server and return the response string.'''
        raise CommandNotImplemented()
    
    
    def voidcmd(self, command):
        '''Send a simple command string to the server and handle the response. 
        Return nothing if a response code in the range 200–299 is received. Raise an exception otherwise.
        '''
        raise CommandNotImplemented()    
    
    
    def retrbinary(self, command, callback, maxblocksize=None, rest=None):
        '''Not Implemented'''
        raise CommandNotImplemented()
        
    
    def retrlines(self, command, callback=None):
        '''Retrieve a file or directory listing in ASCII transfer mode. 
        command should be an appropriate RETR command (see retrbinary()) or a command such as LIST, NLST or MLSD 
        (usually just the string 'LIST'). The callback function is called for each line, with the trailing CRLF stripped. 
        The default callback prints the line to sys.stdout.
        '''
        raise CommandNotImplemented()    
    
    
    def set_pasv(self, passive_mode=True):
        '''Enable “passive” mode if boolean is true, other disable passive mode.
        Set to True (ON) by default
        '''
        raise CommandNotImplemented()    
    
    
    def storbinary(self, cmd=None, file_handle=None, blocksize=None, callback=None):
        '''Used to send a file to the remote server.
        cmd is not used and will be ignored.
        blocksize and callback are not currently implemented.
        '''
        expected_duration = int(os.stat(file_handle.name)[stat.ST_SIZE] /1000)/self.min_dl_rate_kbps
        if expected_duration < 5:
            expected_duration = PROCESS_TIMEOUT_SEC
            
            sftp_cmd = 'put %s' % (file_handle.name) + self.endline
            self.proc.stdin.write(sftp_cmd)
            self.proc.stdin.flush()
            
            cmd_prompt = 'psftp>'
            # --> use timeout on wait4prompt function
            prompt_found = timeout(self._wait_for_text,args=(cmd_prompt,), timeout_duration=expected_duration, default=False )
            
            if not prompt_found:
                self.kill()
                self.proc.wait()
                raise ProcessTimeout('Process took longer than expected, transfer failed')    
            
    
    def storlines(self, command, file, callback=None):
        '''Store a file in ASCII transfer mode. 
        command should be an appropriate STOR command (see storbinary()). 
        Lines are read until EOF from the open file object file using its readline() method to provide the data to be stored. 
        callback is an optional single parameter callable that is called on each line after it is sent.
        '''
        raise CommandNotImplemented()
    
    
    def transfercmd(self, command, rest=None):
        '''Not Implemented'''
        raise CommandNotImplemented()
    
    
    def ntransfercmd(self, command, rest=None):
        '''Not Implemented'''
        raise CommandNotImplemented()
    
    
    def nlst(self, argument='.', return_line_dict=False):
        '''Return a list of files as returned by the NLST command. 
        The optional argument is a directory to list (default is the current server directory). 
        Multiple arguments can be used to pass non-standard options to the NLST command.
        
        argument == path to list, defaults to current server directrory
        return_line_dict == When True returns the 
        return_line_dict dictionary structure:
        { 
        'perm': <unix permissions string>,
        'owner': <unix file/dir owner>,
        'group': <unix group>,
        'size': <file/dir size>,
        'date': <date string>,
        'dirfile': <file/dir name>
        }
        
        psftp command: None
        '''
        path = argument
        cmd = 'nlst'
        files = []
        line_dict = []
        dir_str = self.dir(path, return_as_str=True)
        
        if dir_str:
            # may need to update this string
            # --> Server may return different format?
            regex = r'''(?P<perm>[-rwx]*)\s*(?P<dirs>\d*)\s*(?P<owner>\w*)\s*(?P<group>\w*)\s*(?P<size>\d*)\s*(?P<date>\w{3}\s*\d*\s*\d{4})\s(?P<dirfile>.*)$'''
            for line in dir_str.split(self.endline):
                m = re.match(regex, line)
                if m:
                    temp_line_dict = m.groupdict()
                    if temp_line_dict:
                        # Changes selected items to int
                        temp_line_dict['dirs'] = int(temp_line_dict['dirs'])
                        temp_line_dict['size'] = int(temp_line_dict['size'])
                        
                        line_dict.append(temp_line_dict)
                        files.append(temp_line_dict['dirfile'])
        else:
            msg = 'Command Failed: %s' % (cmd)
            raise Exception(msg)
        
        if return_line_dict:
            return line_dict
        return files
    
    
    def dir(self, argument='.', return_as_str=False):
        '''Produce a directory listing as returned by the LIST command, printing it to standard output. 
        The optional argument is a directory to list (default is the current server directory). 
        Multiple arguments can be used to pass non-standard options to the LIST command. 
        If the last argument is a function, it is used as a callback function as for retrlines(); the default prints to sys.stdout. 
        This method returns None.
        
        argument == path to list, defaults to current server directrory
        return_as_str == When True returns the results of the command as a string
        psftp command: dir
        '''
        path = argument
        dir_str = ''
        wait_for_text = 'psftp>'
    
        cmd = 'dir'
        input_cmd = cmd + ' ' + path + self.endline
        self.proc.stdin.write(input_cmd)
        self.proc.stdin.flush()
        
        # --> use timeout on wait4prompt function
        text_found = timeout(self._wait_for_text, args=(wait_for_text,), timeout_duration=PROCESS_TIMEOUT_SEC, default=False )
        
        if not text_found:
            msg = 'Command Not Successful: "%s"' % (input_cmd)
            raise PsFtpInvalidCommand(msg)            
        
        expected_text = wait_for_text
        dir_str = text_found.replace(expected_text, '').strip()
        if not return_as_str:
            sys.stdout.write(dir_str)
        else:
            return dir_str  
          
    
    def rename(self, fromname, toname):
        '''Rename file fromname on the server to toname.
        psftp command: ren
        '''
        cmd = 'ren'
        raise CommandNotImplemented()
    
    
    def delete(self, filename):
        '''Remove the file named filename from the server. 
        If successful, returns the text of the response, 
        otherwise raises error_perm on permission errors or error_reply on other errors.
        '''
        raise CommandNotImplemented()
            
    
    def cwd(self, pathname=None):
        '''Issue change working directory (cwd) command'''
        if pathname:
            wait_for_text = 'Remote directory is now' # used to determine success
            cmd = 'cd '
            input_cmd = cmd + pathname + self.endline
            self.proc.stdin.write(input_cmd)
            self.proc.stdin.flush()
            
            # --> use timeout on wait4prompt function
            text_found = timeout(self._wait_for_text,args=(wait_for_text,), timeout_duration=PROCESS_TIMEOUT_SEC, default=False )   
            
            if not text_found:
                msg = 'Command Not Successful: "%s"' % (input_cmd)
                raise PsFtpInvalidCommand(msg)
            

    def mkd(self, pathname):
        '''Create a new directory on the server.
        psftp command: mkdir
        '''
        if pathname:
            pathname = self._clean_path(pathname)
            wait_for_text = 'mkdir %s: OK' % (pathname) # used to determine success
            cmd = 'mkdir'
            input_cmd = cmd + ' ' + pathname + self.endline
            self.proc.stdin.write(input_cmd)
            self.proc.stdin.flush()
            
            # --> use timeout on wait4prompt function
            text_found = timeout(self._wait_for_text,args=(wait_for_text,), timeout_duration=PROCESS_TIMEOUT_SEC, default=False )   
            
            if not text_found:
                msg = 'Command Not Successful: "%s"' % (input_cmd)
                raise PsFtpInvalidCommand(msg)        
    
    
    def pwd(self):
        '''Return the pathname of the current directory on the server.
        psftp command: pwd
        '''
        pwd = None
        wait_for_text = self.endline # wait for the endline
        cmd = 'pwd'
        input_cmd = cmd + self.endline
        self.proc.stdin.write(input_cmd)
        self.proc.stdin.flush()
        
        # --> use timeout on wait4prompt function
        text_found = timeout(self._wait_for_text, args=(wait_for_text,), timeout_duration=PROCESS_TIMEOUT_SEC, default=False )
        
        if not text_found:
            msg = 'Command Not Successful: "%s"' % (input_cmd)
            raise PsFtpInvalidCommand(msg)            
        
        expected_text = 'Remote directory is'
        if expected_text in text_found:
            pwd = text_found.replace(expected_text, '').strip()
        else:
            msg = 'Command failed: %s' % (cmd)
            raise Exception(msg)
        return pwd
    
    
    def rmd(self, dirname):
        '''Remove the directory named dirname on the server.
        psftp command: rmdir
        '''
        raise CommandNotImplemented()
    
    
    def size(self, filename):
        '''Request the size of the file named filename on the server. On success, the size of the file is returned as an integer, otherwise None is returned. 
        Note that the SIZE command is not standardized, but is supported by many common server implementations.  
        '''
        raise CommandNotImplemented()

    
    def quit(self):
        '''Quit/Exit psftp if it's still running, ignore if not'''
        if self.proc.returncode == None:
            exit_cmd = 'exit' + self.endline
            self.proc.stdin.write(exit_cmd)
            self.proc.stdin.flush()
            
            print 'Waiting for process to close...'
            self.proc.wait()
            
            
    def close(self):
        '''Wraps the quit command'''
        self.quit()    
    
            
    def kill(self):
        '''Kill the proces using pywin32 and pid'''
        import win32api
        PROCESS_TERMINATE = 1
        handle = win32api.OpenProcess(PROCESS_TERMINATE, False, self.proc.pid)
        win32api.TerminateProcess(handle, -1)
        win32api.CloseHandle(handle)          
        
        
    def __del__(self):
        quit_success = timeout(self.quit, timeout_duration=5, default=False)
        
        if not quit_success:
            self.kill()
        
        
# Exceptions for this module
class PsFtpInvalidCommand(Exception):
    def __init__(self, value):
        self.value = value
        self.message = repr(value)
        
    def __str__(self):
        return repr(self.value)    
    
      
class CommandNotImplemented(Exception):
    def __init__(self, value):
        self.value = "Feature not implemented" + repr(value)
        self.message = repr(value)
        
    def __str__(self):
        return repr(self.value)  
    
        
class ProcessTimeout(Exception):
    def __init__(self, value):
        self.value = value
        self.message = repr(value)
        
    def __str__(self):
        return repr(self.value)              
    

        
