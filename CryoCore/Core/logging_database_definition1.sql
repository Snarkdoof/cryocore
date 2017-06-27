-- EMPTY TABLE QUERIES
-- First, we clean the whole database
-- This part is removable
DELETE FROM log;


-- DROP TABLE QUERIES
-- This part is removable
DROP TABLE log;

-- ########################################################
-- TABLE CREATION QUERIES
-- ########################################################

CREATE TABLE log (
    id SERIAL PRIMARY KEY,              -- Table key            
    message TEXT,                       -- Message included with the log
    level TEXT NOT NULL,                -- level: 'DEBUG', 'INFO','WARNING','ERROR', 'CRITICAL', 'NOTSET'
    time FLOAT,                         -- when the log was generated (secs)
    msecs FLOAT,                        -- when the log was generated (msecs)
    line INTEGER NOT NULL,              -- where the log was generated (line)
    function TEXT,                      -- where the log was generated (function)
    module TEXT NOT NULL,               -- where the log was generated (file)
    logger TEXT NOT NULL,               -- name of the log which generated the log
    CONSTRAINT level_type CHECK (level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'NOTSET')));

-- Second, we grant privileges to the 'aircraft' user
-- Warning: the user 'aircraft' has been already created.

GRANT insert, select, delete on log to pilot;



-------------
Can cut'n'paste this
CREATE TABLE log (
    id SERIAL PRIMARY KEY,
    message TEXT,
    level INTEGER NOT NULL,
    time FLOAT,
    msecs FLOAT,
    line INTEGER NOT NULL,
    function TEXT,
    module TEXT NOT NULL,
    logger TEXT NOT NULL);
