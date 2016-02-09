#! /usr/bin/python
import git
from git import Repo
from git import Git
from ftplib import FTP
import os
import sys
import io
import json


def check_dirs_configured(config, locked_dirs, repo):
    # for each target target_dir - check they are configured to work with this tool
    dirs_missing_commit = []
    for target_dir in config.dirs:
        # if no such file exists (or commit is gibberish) add to list of dirs without .commit files
        if not target_dir.check_valid_commit(repo):
            dirs_missing_commit.append(target_dir)
    
    # if any dirs lack .git_commit files abort with error listing incorrect dirs
    if len(dirs_missing_commit) > 0:
        message = "Invalid / missing commit information for the following directories:\n"
        message += list_dirs(dirs_missing_commit)
        message += "Aborting."
        return abort(locked_dirs, message)
        
    return True

def abort(directories, message):
    for target_dir in directories:
        # perfrm necessary cleanup work in the target_dir
        target_dir.abort()
    # output error
    print (message)
    # die
    return False
    
# Standardized way of listing directories (usually for error messages)
def list_dirs(directories):
    message = ""
    for target_dir in directories:
        message += "\t" + target_dir.path + "\n"
    return message

# Process of locking directories for deploy
def lock_dirs(config):
    locked_dirs = []
    already_locked_dirs = []
    cannot_lock = []
           
    # for each target target_dir - check that they aren't already locked
    for target_dir in config.dirs:
        # connect
        try :
            target_dir.connect()
        except Exception as e:
            cannot_lock.append(target_dir)
            print (e.message)
            continue
        
        # check for lock file
        if target_dir.check_locked():
            already_locked_dirs.append(target_dir)
        else:
            # if not already locked, do so and track which dirs are "locked"
            if target_dir.lock():
                locked_dirs.append(target_dir)
            else:
                cannot_lock.append(target_dir)
        
    # If any lock files already existed, abort with error
    if len(already_locked_dirs) > 0:
        message = "Unable to lock the following target directories:\n"
        message += list_dirs(already_locked_dirs)
        message += "Aborting."
        abort(locked_dirs, message)
        return None
    
    # If we weren't able to lock all the directories, abort with error
    if len(cannot_lock) > 0:
        message = "Unable to lock the following target directories:\n"
        message += list_dirs(cannot_lock)
        message += "Aborting."
        abort(locked_dirs, message)
        return None
    
    return locked_dirs

class Config_Details:
    def __init__(self, config_filename):
        # should contain at minimum:
        # path of local git Repo (ideally absolute path, TBD)
        # list of directories to deploy to
            # <optional> mode of access (default=Normal, FTP also supported)
            # <optional> connection authentication details
      
        config_file = open(config_filename, "r")
        config = json.load(config_file)
        
        errors = []
        if "path" in config.keys():
            self.path = config["path"]
        else: 
            errors.append("path is missing")
            
        self.dirs = []
        if "targets" in config.keys():
            for item in config["targets"]:
                if "path" in item.keys():
                    self.dirs.append(Directory(item["path"], item.get("mode"), item.get("auth")))
                else:
                    errors.append("path missing")
        else:
            errors.append("targets are missing") 
            
        config_file.close()
        if len(errors) > 0:
            raise IOError("Parse errors : " + ",".join(errors))       
        
# Represents a directory we are deploying to
class Directory:
    LOCK_FILE = ".git_lock"
    COMMIT_FILE = ".git_commit" 
    
    def __init__(self, path, connection_mode = None, auth = None):
        self.path = path
        self.buffer = ""
        self.connection_mode = None
        self.commit = None
        
        # Callback for use in some remote read operations     
        def _read_to_buffer(data):
            self.buffer += data
        self.read_to_buffer = _read_to_buffer
        
        if connection_mode is not None:
            # should just be a string, make comparissons case insensitive
            self.connection_mode = connection_mode.upper()
            self.auth = auth
    
    # Makes the connection (if any needed) to the destination directory. 
    # Throws an exception if unable to connect for some reason.
    def connect(self):
        if self.connection_mode is None:
            return
        
        #supported modes : FTP (TODO support more, e.g. SSH, SFTP)
        if self.connection_mode == "FTP":
            if "host" not in self.auth.keys():
                print ("No host")
                raise Exception("Unable to connect via FTP without a host")
            
            if "user" in self.auth.keys() and "password" in self.auth.keys():
                print ("Username and password specified")
                # This logs in automatically
                self.handle = FTP(self.auth['host'],
                                  self.auth['user'], 
                                  self.auth['password'])
                print ("Logged in")
            else:
                print ("No username or password")
                self.handle = FTP(self.auth['host'])
                self.handle.login()     
        else:
            raise Exception("Unsupported connection mode: " + self.connection_mode)        
    
    def check_locked(self):
        # check for lock file
        return self._check_file_exists(self.LOCK_FILE)
    
    # check for the existence of a single file
    def _check_file_exists(self, filename):
        # How we check depends on what connection we have to the filesystem
        if self.connection_mode is None:
            # just a normal directory
            if os.path.exists(os.path.join(self.path, filename )):
                return True
        elif self.connection_mode == "FTP":
            self.handle.cwd(self.path)
            for path_details in self.handle.mlsd():
                if path_details[0] == filename:
                    return True
        return False
    
    def _read_root_dir_file_contents(self, filename):
        contents = ""
        if self.connection_mode is None:
            #get the file in a fairly straightforward way
            file_handle = open(os.path.join(self.path, filename), "r")
            for line in file_handle:
                contents += line
            file_handle.close()
        elif self.connection_mode == "FTP":
            self.buffer = ""
            self.handle.cwd(self.path)
            self.handle.retrbinary('RETR ' + filename, self._read_to_buffer)
            contents = self.buffer
        return contents[:-1]
    
    def check_valid_commit(self, repo):
        # check for a .commit file 
        if not self._check_file_exists(self.COMMIT_FILE):
            return False

        # make sure recorded commit is valid
        commit_id = self._read_root_dir_file_contents(self.COMMIT_FILE)
        self.commit = repo.commit(commit_id) 
        return self.commit and self.commit.__class__ is git.Commit
    
    def lock(self):
        try:
            #write a lock file to the directory - return boolean indicating success
            self.write_new_file(self.LOCK_FILE, "locked")
        except IOError as e:
            print ("Unable to write file: " + e.message)
            return False
        return True
    
    def write_new_file(self, filename, contents):
        print (self.connection_mode)
        if self.connection_mode is None:
            path = os.path.join(self.path, filename)
            w = open(path, "w")
            w.write(contents)    
            w.close()
        elif self.connection_mode == "FTP":
            in_stream = io.BytesIO(contents)
            path = os.path.join(self.path, os.path.dirname(filename))
            print(self.path)
            try:
                self.handle.cwd(self.path)
            except IOError as e:
                print ("Error : " + e.message)
                self.handle.mkd(path)
                self.handle.cwd(path)
            self.handle.storbinary('STOR ' + os.path.basename(filename), in_stream)
            in_stream.close()
            
    def copy_file(self, filename, source_file):
        if self.connection_mode is None:
            path = os.path.join(self.path, filename)
            dirname = os.path.dirname(path)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            w = open(path, "wb")
            f = open(source_file, "rb")
            for line in f:
                w.write(line)    
            w.close()
        elif self.connection_mode == "FTP":
            stream = io.open(source_file, "rb")
            path = os.path.join(self.path, os.path.dirname(filename))
            basename = os.path.basename(filename)
            try:
                self.handle.cwd(path)
            except IOError:
                self.handle.mkd(path)
                self.handle.cwd(path)
            self.handle.storbinary('STOR ' + basename, stream)
            stream.close()
            
    def rename_file(self, oldname, newname):
        if self.connection_mode is None:
            pass
        elif self.connection_mode == "FTP":
            self.handle.cwd(self.path)
            self.handle.rename(oldname, newname)
            
    def delete_file(self, filename):
        # delete lock file - if exist
        if self.check_locked():
            if not self.connection_mode:
                path = os.path.join(self.path, filename)
                if os.path.exists(path):
                    os.remove(path)
            elif self.connection_mode == "FTP":
                directory = os.path.dirname(filename)
                basename = os.path.basename(filename)
                self.handle.cwd(os.path.join(self.path, directory))
                self.handle.delete(basename)
    
    def abort(self):
        self.delete_file(self.LOCK_FILE)
        # if we have a connection, close it.
        if self.connection_mode is None:
            return
        elif self.connection_mode == "FTP":
            self.handle.quit()
    
    def deploy_diff(self, diff, config):
        for item in diff:
            # delete all files that have been removed (exist in that commit and not local commit)
            if item.deleted_file:
                self.delete_file(str(item.a_path))
            # if the item has been renamed, do that
            elif item.rename_from and item.rename_to:
                self.rename_file(str(item.rename_from), 
                                 str(item.rename_to))
            else:
                # copy over all files that have been added or changed    
                self.copy_file(str(item.a_path), 
                                    str(os.path.join(config.path, item.a_path)))
    
    # Instead of deploying a diff, deploy everything in the tree.
    def deploy_tree(self, tree, config):
        for path in tree:
            self.copy_file(str(path), str(os.path.join(config.path, path)))
        
    def deploy(self, commit, config):
        # diff between commit id and local repo's last pushed commit
        if self.commit is not None:
            diff = self.commit.diff(commit)
            self.deploy_diff(diff, config)
        else:
            g = Git( config.path )
            self.deploy_tree(g.ls_files().split("\n"), config)
        
        # update .commit file in target target_dir
        self.write_new_file(self.COMMIT_FILE, commit.hexsha)
        # delete lock file
        self.delete_file(self.LOCK_FILE)
        return True
    
if __name__ == "__main__":
    # Accept command line arguments
    flags = []
    commit_id = None
    for arg in sys.argv[1:]:
        if arg[0] == "-":
            flags.append(arg[1:])
        else:
            commit_id = arg[0]
            
    # Get config object, should have path of local repo and dirs to deploy to
    try :
        config = Config_Details("git_deploy.config")
    except IOError as e:
        print (e.message)
        print ("Unable to read git_deply.config. Aborting.")
        exit()
    
    locked_dirs = lock_dirs(config)
    if not locked_dirs:
        exit()
    
    # check to see if the path in the config is actually a repo...
    repo = Repo(config.path)
    
    if repo.__class__ is not Repo:
        message = "No repository found at " + config.path + "\n"
        message += "Aborting."
        abort(locked_dirs, message)
        exit()
    
    # either we have the commit specified or we use the head
    if commit_id is not None:
        commit = repo.commit(commit_id)
    else:
        commit = repo.head.commit
    
    # if the local commit specified is somehow invalid, abort.
    if commit.__class__ is not git.Commit: 
        abort(locked_dirs, "Invalid local commit for deploy\n")
    
    # Allow the -h option to make this a "hard" deploy, ignore pre-set configuration
    if "h" not in flags:
        if not check_dirs_configured(config, locked_dirs, repo):
            exit()
            
    # for each target target_dir - get a diff between commit and local git Repo
    for target_dir in config.dirs:
        if target_dir.deploy(commit, config):
            print "Deployed successfully to ", str(target_dir.path)
            
    print ("Done")