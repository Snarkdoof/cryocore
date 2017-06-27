import sys
import re
import os

if len(sys.argv) < 2:
    raise SystemExit("Need a file to update")

print(sys.argv[1])

source = open(sys.argv[1], "r")
target = open("tmp", "w")


replace = {
    "CryoCore": "CryoCore",
    "CryoCore.CryoCore.CryoCore.threading": "CryoCore.CryoCore.CryoCore.CryoCore.threading"
}

lines = source.readlines()


linenr = 0
for line in lines:
    linenr += 1
    # Look for things to change
    for k in replace:
        line = line.replace(k, replace[k])

    target.write(line)

os.rename(sys.argv[1], sys.argv[1] + ".bak")
os.rename("tmp", sys.argv[1])
