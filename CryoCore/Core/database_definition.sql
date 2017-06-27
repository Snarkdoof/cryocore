CREATE TABLE log (
    id INTEGER PRIMARY KEY,
    message TEXT,
    level TEXT NOT NULL,
    time FLOAT,
    msecs FLOAT,
    line INTEGER NOT NULL,
    function TEXT,
    module TEXT NOT NULL,
    logger TEXT NOT NULL,
    CONSTRAINT level_type CHECK (level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'NOTSET')));


