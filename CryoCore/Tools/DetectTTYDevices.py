from CryoCore.Core.Utils import list_tty_devices

print("Connected TTY Devices:")

devices = list_tty_devices()

for dev in devices:
    print(dev["ID_MODEL"] + ": " + dev["DEVNAME"] + " (%s:%s)"%(dev["MAJOR"], dev["MINOR"]))

print()
