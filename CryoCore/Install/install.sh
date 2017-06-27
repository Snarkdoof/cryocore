#!/bin/bash
export PYTHONPATH=.

if ! test -d "CryoCore" ; then
  echo "Must run install from the correct directory (the one with CryoCore in it)"
  exit -1
fi


uname -v | grep Ubuntu

if [ $? == 0 ] || [ $1 == "force" ]; then
	echo "Checking dependencies"
	sudo apt-get install mysql-server mysql-client lm-sensors ntp python-argcomplete python3-argcomplete

	sudo activate-global-python-argcomplete
	
	echo "Installing mysql connector"
	sudo dpkg -i CryoCore/Install/libs/mysql-connector-python*.deb

	echo "Detecting sensors"
	sudo sensors-detect
else
	echo "Not Ubuntu, please ensure that you have mysql installed"
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
python3 CryoCore/Tools/ConfigTool.py import CryoCore/Install/defaultConfiguration.xml

echo "OK"
