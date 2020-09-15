CREATE TABLE "public"."expired" (
    "id" text NOT NULL,
    "task" int NOT NULL,
    "ts" bigint NOT NULL,
    CONSTRAINT "expired_pkey" PRIMARY KEY("id", "task")
) WITH (oids = false);

CREATE TABLE "public"."source" (
    "id" bigint NOT NULL,
    "ts" bigint NOT NULL,
    "data" bytea NOT NULL,
    "stimulus" bigint NOT NULL,
    "stimulus_expired" bigint,
    "sent" boolean DEFAULT false NOT NULL,
    "expired" boolean DEFAULT false NOT NULL
) WITH (oids = false);

CREATE TABLE "public"."sink" (
    "id" bigint NOT NULL,
    "ts" bigint NOT NULL,
    "data" bytea NOT NULL,
    "stimulus" bigint NOT NULL,
    "sent" boolean DEFAULT false NOT NULL
) WITH (oids = false);

CREATE TABLE "public"."edge" (
    "source" bigint NOT NULL,
    "sink" bigint NOT NULL,
    "stimulus" bigint NOT NULL,
    "sent" boolean DEFAULT false NOT NULL
) WITH (oids = false);

CREATE INDEX "source_ts" ON "public"."source" ("ts");
CREATE INDEX "source_expired" ON "public"."source" ("expired");
CREATE INDEX "source_sent" ON "public"."source" ("sent");
CREATE INDEX "sink_sent" ON "public"."sink" ("sent");
CREATE INDEX "edge_sent" ON "public"."sink" ("sent");

