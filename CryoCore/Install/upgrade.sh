#!/bin/bash

echo "Upgrade databases?"
read YN
if test "$YN" == "yes"; then
  echo "Upgrading status database"
  python CryoCore/Core/Status/MySQLReporter.py upgrade

  echo "Upgrading config database"
  python3 CryoCore/Core/Config.py upgrade
fi

