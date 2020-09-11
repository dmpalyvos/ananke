-- Out of date
BEGIN;
WITH usink AS (UPDATE sink SET sent = true WHERE sent = false RETURNING id, ts, stimulus, data) SELECT DISTINCT * FROM usink;
WITH usource AS (UPDATE source SET sent = true WHERE sent = false RETURNING id, ts, stimulus, data) SELECT DISTINCT * FROM usource;
WITH uedge AS  (UPDATE edge SET sent = true WHERE sent = false RETURNING source, sink, stimulus) SELECT DISTINCT * FROM uedge;
WITH uack AS (UPDATE source SET expired = true
    WHERE source.ts <= (SELECT MIN(ts) FROM expired) AND SOURCE.sent = true AND source.expired = false
    RETURNING id, ts, stimulus, data) SELECT DISTINCT * FROM uack;
COMMIT;