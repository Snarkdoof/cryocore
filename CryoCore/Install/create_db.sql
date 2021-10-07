CREATE DATABASE IF NOT EXISTS cryocore;
CREATE USER IF NOT EXISTS cc@localhost identified by 'Kjøkkentrappene bestyrer sørlandske databehandlingsrutiner';
GRANT ALL ON cryocore.* TO 'cc'@'localhost';
FLUSH PRIVILEGES;

