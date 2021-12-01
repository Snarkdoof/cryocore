#!/bin/bash
export PYTHONPATH=.
export LC_ALL="C.UTF-8" 

if ! test -d "CryoCore" ; then
  echo "Must run install from the correct directory (the one with CryoCore in it)"
  exit -1
fi

sudo locale-gen "nb_NO.UTF-8"
sudo locale-gen "en_US.UTF-8"

# Check for mysql
apt show mysql-serverf 2>/dev/null > /dev/null
if [[ $? == 0 ]]; then 
	MYSQL="mysql";
else
	MYSQL="mariadb"
fi

python2 --version 2>/dev/null
if [[ $? == 0 ]]; then
	PYTHON2="yes"
else
	PYTHON2="no"
fi

echo "Using database $MYSQL, python2 support: $PYTHON2" 

apt-get --help > /dev/null 
if [[ $? == 0 ]] || [[ $1 == "force" ]]; then
	echo "Checking dependencies"
  sudo apt-get update
	sudo apt-get -y install $MYSQL-server $MYSQL-client
	if [[ $? != 0 ]]; then
		echo "Could not install database '$MYSQL"
	fi

	# python2 stuff
	if [[ $PYTHON2 == "yes" ]]; then
		sudo apt-get -y install python-pip python-pyinotify python-psutil
		sudo pip install mysql-connector-python argcomplete
	fi

	# python3 stuff & more
	sudo apt-get -y install lm-sensors ntp python3-argcomplete python3-pip python3-pyinotify python3-psutil bash-completion

	sudo activate-global-python-argcomplete
	
	echo "Installing mysql connector"
  sudo pip3 install mysql-connector-python

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

# Check if it's mariadb
sudo mysql < CryoCore/Install/create_db.sql
# or mysql
# mysql -u root -p < CryoCore/Install/create_db.sql

echo "Importing default config"
./bin/ccconfig import CryoCore/Install/defaultConfiguration.xml

DIR=`pwd`
echo "Add pythonpath and cryocore tools to .bashrc?"
read -n 1 yn
if [ "$yn" == "y" ]; then
	echo ""
	echo "export PYTHONPATH=\$PYTHONPATH:$DIR:." >> ~/.bashrc	
	echo "export PATH=\$PATH:$DIR/bin" >> ~/.bashrc
	echo "run 'source ~/.bashrc' or log in again to use CryoCore tools"
fi
echo ""


echo "Copying startup scripts"
python3 -c "import sys;import os;lines=sys.stdin.read();print(lines.replace('CCINSTALLDIR', os.getcwd()))" < CryoCore/Install/cryocore.service > /tmp/cryocore.service
sudo mv /tmp/cryocore.service /etc/systemd/system/

python3 -c "import sys;import os;lines=sys.stdin.read();print(lines.replace('CCINSTALLDIR', os.getcwd()))" < CryoCore/Install/cryocored > bin/cryocored
chmod 755 bin/cryocored

systemctl is-enabled cryocore.service
if test $? == 1; then
  echo "To autostart, please write:"
  echo "sudo systemctl enable cryocore.service"
fi

echo "OK"
