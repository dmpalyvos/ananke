MATCH (n) DETACH DELETE n;
DROP INDEX sink_uid;
DROP INDEX source_uid;
CREATE INDEX sink_uid FOR (s:SINK) on (s.uid);
CREATE INDEX source_uid FOR (s:SOURCE) on (s.uid);
