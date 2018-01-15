#!/bin/bash
export PYTHONPATH=.
export LC_ALL="C.UTF-8" 

if ! test -d "CryoCore" ; then
  echo "Must run install from the correct directory (the one with CryoCore in it)"
  exit -1
fi

sudo locale-gen "nb_NO.UTF-8"
sudo locale-gen "en_US.UTF-8"

apt-get --help > /dev/null 
if [[ $? == 0 ]] || [[ $1 == "force" ]]; then
	echo "Checking dependencies"
  sudo apt-get update
	sudo apt-get install mysql-server mysql-client lm-sensors ntp python-argcomplete python3-argcomplete python-pip python3-pip python-pyinotify python3-pyinotify python-psutil python3-psutil

	sudo activate-global-python-argcomplete
  sudo activate-global-python-argcomplete3
	
	echo "Installing mysql connector"
  sudo pip install mysql-connector==2.1.4
  sudo pip3 install mysql-connector==2.1.4
	# sudo dpkg -i CryoCore/Install/libs/mysql-connector-python*.deb

	echo "Detecting sensors"
	sudo sensors-detect
else
	echo "Not Debian based, please ensure that you have mysql installed"
	echo "This also includes mysql-connector, look in CryoCore/Install/libs/"
	echo "Continue with install?"
	read -n 1 yn
	if [ "$yn" != "y" ]; then
		exit;
	fi
fi

echo "Enter password for mysql admin:"
mysql -u root -p < CryoCore/Install/create_db.sql

echo "Importing default config"
python CryoCore/Tools/ConfigTool.py import CryoCore/Install/defaultConfiguration.xml


echo "Copying startup scripts"
python -c "import sys;import os;lines=sys.stdin.read();print lines.replace('CCINSTALLDIR', os.getcwd())" < CryoCore/Install/cryocore.service > /tmp/cryocore.service
sudo mv /tmp/cryocore.service /etc/systemd/system/

python -c "import sys;import os;lines=sys.stdin.read();print lines.replace('CCINSTALLDIR', os.getcwd())" < CryoCore/Install/cryocored > bin/cryocored
chmod 755 bin/cryocored

systemctl is-enabled cryocore.service
if test $? == 1; then
  echo "To autostart, please write:"
  echo "sudo systemctl enable cryocore.service"
fi

echo "OK"
