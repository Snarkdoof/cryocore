#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK

"""
Commandline tool to get and set config parameters while running
"""
from __future__ import print_function

import sys
import time

import xml.dom.minidom
from argparse import ArgumentParser

try:
    import argcomplete
except:
    print("Missing argcomplete, autocomplete not available")

from CryoCore import API
import CryoCore.Core.Config as Config


class XMLImport:
    def parse_file(self, filename):
        xml = open(filename, "r").read()
        return self.parse_string(xml)

    def parse_string(self, string):
        root = xml.dom.minidom.parseString(string)
        # Find the "configuration" block

        config = root.getElementsByTagName("configuration")[0]
        return self._parse(config)

    def _parse(self, root, path=""):
        retval = []
        my_name = ".".join([path, root.nodeName]).replace(".configuration.", "")

        for child in root.childNodes:
            if child.nodeType == root.TEXT_NODE:
                if child.nodeValue.strip():
                    return [(my_name, child.nodeValue)]
            else:
                retval += self._parse(child, my_name)
        return retval


def usage():
    return """%s <command> <parameter> <value>
    Where command is one of:
       reset - Remove 'temporary' data from database to prepare for a new run of the software
       get <parameter> - get a value
       set <parameter> <value> - set a value
       add <parameter> <value> - add a value
       remove <parameter> - remove a parameter recursively
       delete version <version> - delete a full configuration
       list [parameter] - list children of this parameter
       versions [partial name]- list all known versions (containing partial name)
       import <xml_file> <version> - import XML file as config version
       serialize <root> - Serialize config below a given root (optional) as JSON
       deserialize <root> <filename>  - Deserialize config to a given root

       TIP: Set the default configuration by running
          %s set default_version VERSION_NAME -v default
    """ % (sys.argv[0], sys.argv[0])


def yn(message):
    """
    Present a yes/no question, return True iff yes
    """
    print(message)
    if sys.version_info.major == 3:
        response = input().strip()
    else:
        response = raw_input().strip()
    if response.lower() == "y" or response.lower() == "yes":
        return True

if __name__ == "__main__":
    debug_ac = False
    try:
        def completer(prefix, parsed_args, **kwargs):
            if debug_ac:
                cfg = API.get_config()
            else:
                cfg = API.get_config(version=parsed_args.version)

            r = prefix.rfind(".")
            if r == -1:
                root = ""
                p = ""
            else:
                root = prefix[:r]
                p = root + "."
            print("Get config", root)
            elems = cfg.get(root).children
            ret = ['reset', 'get', 'set', 'add', 'list', 'versions', 'import', 'serialize', 'deserialize', 'remove']
            add = 0
            for elem in elems:
                print("**", p + elem.name, prefix)
                if (p + elem.name).startswith(prefix):
                    add += 1

            for elem in elems:
                if not (p + elem.name).startswith(prefix):
                    continue
                if elem.datatype == "folder":
                    print(p + elem.name, prefix)
                    if p + elem.name == prefix or add < 2:
                        print("Going recursive for", p + elem.name)
                        ret.extend(completer(p + elem.name + ".", parsed_args, recursive=True))
                    else:
                        ret.append(p + elem.name)
                else:
                    ret.append(p + elem.name)
            return ret

        parser = ArgumentParser(description="View and update CryoCore configuration", usage=usage())
        parser.add_argument('parameters', type=str, nargs='+', help='Parameters to list/modify').completer = completer
        parser.add_argument("-v", "--version", dest="version",
                            default=None,
                            help="Version to operate on")

        parser.add_argument("--overwrite", dest="overwrite",
                            action="store_true", default=False,
                            help="Always overwrite")

        parser.add_argument("--neg", dest="negate",
                            action="store_true", default=False,
                            help="negate")

        parser.add_argument("--db_name", type=str, dest="db_name", default="", help="cryocore or from .config")
        parser.add_argument("--db_user", type=str, dest="db_user", default="", help="cc or from .config")
        parser.add_argument("--db_host", type=str, dest="db_host", default="", help="localhost or from .config")
        parser.add_argument("--db_password", type=str, dest="db_password", default="", help="defaultpw or from .config")

        if "argcomplete" in sys.modules:
            argcomplete.autocomplete(parser)

        options = parser.parse_args()
        if options.overwrite:
            print("WARNING: Overwrite is set")

        db_cfg = {}
        if options.db_name:
            db_cfg["db_name"] = options.db_name
        if options.db_user:
            db_cfg["db_user"] = options.db_user
        if options.db_host:
            db_cfg["db_host"] = options.db_host
        if options.db_password:
            db_cfg["db_password"] = options.db_password

        if len(db_cfg) > 0:
            API.set_config_db(db_cfg)
        cfg = API.get_config()  # db_cfg=db_cfg)

        if options.version:
            try:
                cfg.set_version(options.version)
            except Config.NoSuchVersionException:
                if yn("No version '%s', create it?" % options.version):
                    cfg.set_version(options.version, create=True)
                else:
                    raise SystemExit("Aborted")

        if debug_ac:
            print(completer(sys.argv[len(sys.argv) - 1], None))
            raise SystemExit()

        args = options.parameters

        if args:
            command = args[0]
        else:
            print("SHOULD NEVER GET HERE")
            usage()
            raise SystemExit()

        if args[0] == "get":
            if len(args) < 2:
                usage()
            param = cfg.get(args[1])
            if not param:
                print("No such parameter '%s'" % args[1])
                raise SystemExit(1)
            print(param.get_full_path())
            print(" Value        : %25s" % param.get_value())
            print(" Datatype     : %25s" % param.datatype)
            print(" Version      : %25s" % param.version)
            print(" Last modified: %25s" % time.ctime(param.last_modified))
            print(" Comment      : %25s" % param.comment)
            print()

        elif args[0] == "set":
            if len(args) < 3:
                usage()
            if options.negate:
                if (args[2].find(".") > -1):
                    val = -float(args[2])
                else:
                    val = - int(args[2])
            else:
                val = args[2]
            cfg.set(args[1], val, version=options.version, check=False)

        elif args[0] == "add":
            if len(args) < 3:
                usage()
            if options.negate:
                if (args[2].find(".") > -1):
                    val = -float(args[2])
                else:
                    val = - int(args[2])
            else:
                val = args[2]
            cfg.add(args[1], val, version=options.version)
        elif args[0] == "remove":
            if len(args) != 2:
                usage()
            try:
                cfg.remove(args[1], version=options.version)
            except:
                pass

        elif command == "list":
            if len(args) > 1:
                root = args[1]
            else:
                root = "root"

            def recursive_print(root, indent=""):
                elems = cfg.keys(root)
                for elem in elems:
                    recursive_print(root + "." + elem, indent + "  ")

                if len(elems) == 0:
                    print(root, "=", cfg[root])

            recursive_print(root)
        elif command == "reset":
            cfg.reset()

        elif command == "versions":
            if len(args) > 1:
                versions = cfg.list_versions(args[1])
            else:
                versions = cfg.list_versions()
            print("Known versions:")
            for version in versions:
                print("  ", version)
            print()
        elif command == "delete":
            if len(args) < 2:
                raise SystemExit("What do you want me to delete?")
            if args[1] == "version":
                if not options.version:
                    raise SystemExit("Need a version to delete")
                if yn("Delete configuration version '%s' permanently?" % options.version):
                    cfg.delete_version(options.version)
                    print("Configuration deleted!")
                else:
                    print("Aborted")
            else:
                print("Dont know how to delete '%s'" % args[1])

        elif command == "import":
            if len(args) < 2:
                raise SystemExit("Need xml file to import")
            num_params = 0
            parser = XMLImport()
            params = parser.parse_file(args[1])
            for (param, value) in params:
                print("ADDING", param, value)
                cfg.add(param, value, version=options.version, overwrite=True)
                num_params += 1

            print("Imported %d parameters" % num_params)
        elif command == "serialize":
            if len(args) > 1:
                root = args[1]
            else:
                root = ""

            print(cfg.serialize(root=root, version=options.version))

        elif command == "deserialize":
            if len(args) > 1:
                root = args[1]
            else:
                root = ""

            if len(args) > 2:
                data = open(args[2], "r").read()
            else:
                data = sys.stdin.read()
            cfg.deserialize(data, root=root, version=options.version,
                            overwrite=options.overwrite)
        elif command.find("=") > -1:
            # Assignment
            name, value = command.split("=")
            cfg.set(name, value, version=options.version, check=True)
        else:
            res = []
            if args[0][-1] == ".":  # likely result of auto-complete
                args[0] = args[0][:-1]
            try:
                def rec_lookup(pname):
                    r = []
                    param = cfg.get(pname)
                    if (param.datatype == "folder"):
                        for c in param.children:
                            r.extend(rec_lookup(pname + "." + c.name))
                    else:
                        r.append(param)
                    return r
                res = rec_lookup(args[0])
            except Exception as e:
                res = cfg.search(args[0])
            for param in res:
                print(param.get_full_path(), "=", param.get_value())
    except Exception as e:
        import traceback
        traceback.print_exc()
        print("", e)
    finally:
        API.shutdown()
