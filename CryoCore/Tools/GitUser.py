#!/usr/bin/env python

import os
import sys
import curses
import locale
import json
import traceback
import io
from argparse import ArgumentParser

locale.setlocale(locale.LC_ALL, '')
code = locale.getpreferredencoding()
ui = None
def startUI(screen):
    ui.run(screen)

class GitUserDB:
    def __init__(self):
        try:
            os.mkdir(os.path.expanduser("~/.norut-git-users/"))
        except:
            pass
        self.users = {}
        try:
            self.read_database()
        except:
            sys.stderr.write("Warning: Database not found.\n")
        
    def read_database(self):
        with io.open(os.path.expanduser("~/.norut-git-users/users.json"), "r", encoding="utf8") as fd:
            self.users = json.load(fd)

    def write_database(self):
        with io.open(os.path.expanduser("~/.norut-git-users/users.json"), "w", encoding="utf8") as fd:
            data = json.dumps(self.users, ensure_ascii=False, indent=2, sort_keys=True, encoding="utf8")
            fd.write(unicode(data))
    
    def select_user(self, nick):
        print os.path.expanduser("~/.norut-git-users/") + nick
    
    def add_user(self, nick, name, email, key):
        self.users[nick] = { "name" : name, "email" : email, "key" : key }
        self.write_database()
    
    def list_users(self, index_or_nick=None):
        for key in self.users.keys():
             sys.stderr.write(key+"\n")
    
    def unset_user(self):
        sys.stderr.write("Source following to unset GIT environment variables")
        print os.path.expanduser("~/.norut-git-users/unset-user")
    
    def write_user_script(self, nick, user, fd):
        script = u"""#!/bin/bash
export NORUT_GIT_USER="%s"
export NORUT_GIT_SSH_KEY="%s"
export GIT_SSH="%s"
export GIT_AUTHOR_NAME='%s'
export GIT_AUTHOR_EMAIL='%s'
""" % (nick, user["key"], os.path.expanduser("~/.norut-git-users/ssh-wrapper"), user["name"], user["email"])
        fd.write(script)

    def update_scripts(self):
        # Write ssh-wrapper script
        with io.open(os.path.expanduser("~/.norut-git-users/ssh-wrapper"), "w", encoding="utf8") as fd:
            script = u"""#!/bin/bash
exec ssh -i $NORUT_GIT_SSH_KEY "$@"
"""
            fd.write(script)
        # Write unset-user script
        with io.open(os.path.expanduser("~/.norut-git-users/unset-user"), "w", encoding="utf8") as fd:
            script = u"""#!/bin/bash
unset NORUT_GIT_USER
unset NORUT_GIT_SSH_KEY
unset GIT_SSH
unset GIT_AUTHOR_NAME
unset GIT_AUTHOR_EMAIL
"""
            fd.write(script)
        
        os.chmod(os.path.expanduser("~/.norut-git-users/ssh-wrapper"), 0744)
        os.chmod(os.path.expanduser("~/.norut-git-users/unset-user"), 0744)
        for key in self.users:
            user = self.users[key]
            with io.open(os.path.expanduser("~/.norut-git-users/")+key, "w", encoding="utf8") as fd:
                self.write_user_script(key, user, fd)
            os.chmod(os.path.expanduser("~/.norut-git-users/")+key, 0744)

def get_input(default_value, prompt):
    sys.stderr.write(prompt+"\n"+"> ")
    line = sys.stdin.readline()
    line = line.strip()
    return line if len(line) > 0 else default_value

def add_user(db):
    nick = get_input(None, "Enter nickname")
    name = get_input(None, "Enter real name (used for GIT_AUTHOR_NAME)")
    email = get_input(None, "Enter email address (used for GIT_AUTHOR_EMAIL)")
    key = get_input(None, "Enter absolute path to SSH key")
    if nick and name and email and key:
        db.add_user(nick, name, email, key)
        db.write_database()
    else:
        print "Error: No fields may be left blank; user not created/updated."
    

if __name__ == "__main__":
    parser = ArgumentParser(description="User interface to switch between git users.")
    parser.add_argument("-a", "--add", dest="add", action="store_true", default=False, help="Add a user config")
    parser.add_argument("-u", "--update", dest="update", action="store_true", default=False, help="Update user environment scripts")
    parser.add_argument("-s", "--select", dest="nick", default=None, help="Activate this user (returns path to this user's env script)")
    
    db = GitUserDB()
    options = parser.parse_args()
    if options.add:
        add_user(db)
    elif options.update:
        db.update_scripts()
    elif options.nick != None:
        db.select_user(options.nick)
    else:
        db.list_users()
        index_or_nick = get_input(None, "Enter number or short name of user to activate")
        db.select_user(index_or_nick)
