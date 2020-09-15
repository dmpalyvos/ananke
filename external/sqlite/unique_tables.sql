CREATE TABLE expired (
    id text NOT NULL,
    task integer NOT NULL,
    ts integer NOT NULL,
    stimulus integer NOT NULL);

CREATE TABLE source (
    id integer NOT NULL PRIMARY KEY,
    ts integer NOT NULL,
    data text NOT NULL,
    stimulus integer NOT NULL,
    sent integer DEFAULT false NOT NULL,
    expired integer DEFAULT false NOT NULL);


CREATE TABLE sink (
    id integer NOT NULL PRIMARY KEY,
    ts integer NOT NULL,
    data text NOT NULL,
    stimulus integer NOT NULL,
    sent integer DEFAULT false NOT NULL);

CREATE TABLE edge (
    source bigint NOT NULL,
    sink bigint NOT NULL,
    stimulus bigint NOT NULL,
    sent boolean DEFAULT false NOT NULL,
    FOREIGN KEY (sink) REFERENCES sink(id),
    FOREIGN KEY (source) REFERENCES source(id));

