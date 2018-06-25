CREATE DATABASE cryocore;
CREATE USER 'cc'@'localhost' IDENTIFIED BY 'Kjøkkentrappene bestyrer sørlandske databehandlingsrutiner';
GRANT ALL ON cryocore.* TO 'cc'@'localhost';
FLUSH PRIVILEGES;

