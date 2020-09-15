package io.palyvos.provenance.util;


import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Updates.set;
import static io.palyvos.provenance.util.AbstractDatabaseQueryRunner.DEFAULT_TIMESTAMP;
import static io.palyvos.provenance.util.AbstractDatabaseQueryRunner.TASK_INDEX;
import static io.palyvos.provenance.util.AbstractDatabaseQueryRunner.summarizeBytes;
import static io.palyvos.provenance.util.AbstractDatabaseQueryRunner.trySleep;
import static java.util.Arrays.asList;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import io.palyvos.provenance.ananke.output.NoOpProvenanceGraphEncoder;
import io.palyvos.provenance.ananke.output.ProvenanceGraphEncoder;
import io.palyvos.provenance.ananke.output.TimestampedFileProvenanceGraphEncoder;
import io.palyvos.provenance.genealog.GenealogMongoSink;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AnankeMongoDBQueryRunner {

  private static Logger LOG = LoggerFactory.getLogger(AnankeMongoDBQueryRunner.class);
  private final ExperimentSettings settings;
  private transient Thread thread;
  private transient AnankeMongoDBQueryRunner.Runner runner;
  public static final boolean WRITE_TO_FILE = false;
  private static boolean initialized = false;

  public AnankeMongoDBQueryRunner(ExperimentSettings settings) {
    Validate.notNull(settings, "settings");
    this.settings = settings;
  }

  /**
   * WARNING: Can be safely called multiple times, but only from one JVM. Otherwise there is a
   * possibility for concurrency issues!
   */
  public void start() {
    synchronized (AnankeMongoDBQueryRunner.class) {
      if (initialized) {
        return;
      }
      this.runner = new Runner("mongo", settings, WRITE_TO_FILE);
      this.thread = new Thread(runner);
      thread.setName("MongoDB-Query-Runner");
      thread.start();
      initialized = true;
    }
  }

  public void stop() {
    if (runner != null) {
      runner.stop();
    }
  }

  private static class Runner implements Runnable {

    //    Bsons that are recycled ********************************************
    private static final Bson unsent = eq("sent", 0);
    private static final Bson typeSink = eq("type", "sink");
    private static final Bson typeSource = eq("type", "source");
    private static final Bson typeEdge = eq("type", "edge");

    private static final Bson setUnsentToSend = set("sent", 1);

    private static final Bson typeSafeTS = eq("type", "safe_ts");
    private static final Bson groupMinimum = group(null, Accumulators.min("min", "$ts"));

    private Bson lessThan(long timestamp) {
      return lt("ts", timestamp);
    }

    private static final Bson notExpired = eq("expired", 0);
    private static final Bson sent = eq("sent", 1);


    private static final Bson setToExpired = set("expired", 1);
    // ************************************************************************

    private final long pollFrequencyMillis;
    private transient MongoDatabase database;
    protected final TransactionalProvenanceGraphEncoder encoder;
    private volatile boolean enabled = true;

    protected final AvgStat provenanceReadTimeStatistic;
    protected final TransactionalCountStat provenanceReadsStatistic;
    protected final TransactionalMaxStat deliveryLatencyStatistic;



    public Runner(String name, ExperimentSettings settings,
        boolean writeToFile) {
      Validate.isTrue(settings.pollFrequencyMillis() >= 0, "negative poll frequency");
      this.pollFrequencyMillis = settings.pollFrequencyMillis();
      this.provenanceReadTimeStatistic = new AvgStat(
          settings.provenanceReadTimeFile(TASK_INDEX, name),
          settings.autoFlush());
      this.provenanceReadsStatistic = new TransactionalCountStat(
              new CountStat(settings.provenanceReadsFile(0, name), settings.autoFlush()));
      MaxStat delegateStatistic = new MaxStat(settings.deliveryLatencyFile(TASK_INDEX, name),
          settings.autoFlush());
      this.deliveryLatencyStatistic = new TransactionalMaxStat(
          delegateStatistic);
      AckDeliveryLatencyHelper.setStatistic(delegateStatistic);
      database = GenealogMongoSink.MONGO_CLIENT.getDatabase(GenealogMongoSink.DATABASE_NAME);
      ProvenanceGraphEncoder delegateEncoder =
          writeToFile ? new TimestampedFileProvenanceGraphEncoder(
              settings.outputFile(TASK_INDEX, name), settings.autoFlush())
              : new NoOpProvenanceGraphEncoder();
      this.encoder = new TransactionalProvenanceGraphEncoder(delegateEncoder);
    }


    @Override
    public void run() {
      while (enabled) {
        if (!trySleep(pollFrequencyMillis)) {
          break;
        }
        fetchAndUpdate();
      }
      trySleep(AbstractDatabaseQueryRunner.FINAL_SLEEP_MILLIS);
      fetchAndUpdate();
      provenanceReadTimeStatistic.close();
      encoder.close();
    }


    public void fetchAndUpdate() {


      final long start = System.currentTimeMillis();
      try {
        MongoCollection<Document> anankeCollection = database
            .getCollection(GenealogMongoSink.COLLECTION_NAME);

        AckDeliveryLatencyHelper.reset();
        provenanceReadsStatistic.reset();
        deliveryLatencyStatistic.reset();
        encoder.reset();

        // -------- PART I: Sources, Sinks and Edges

        ArrayList<ObjectId> idsToMarkSent = new ArrayList<>();
        // find all docs for unsent sinks and edges:
        FindIterable<Document> unsentSinksEdgesDocs = anankeCollection
            .find(and(unsent, or(typeSink, typeEdge)));
        // find and deduplicate all source docs, return only those where non of the duplicates was sent
        AggregateIterable<Document> unsentGroupedSourcesDocs = anankeCollection
            .aggregate(asList(
                match(typeSource),
                group("$uid", Accumulators.addToSet("document_ids", "$_id"),
                    Accumulators.max("combined_sent", "$sent")),
                match(eq("combined_sent", 0))
                )
            );

        long currentTimeMillis = System.currentTimeMillis();
        // add sinks and edges to encoder and to latency calculation for them
        for (Document doc : unsentSinksEdgesDocs) {
          idsToMarkSent.add(doc.getObjectId("_id"));
          provenanceReadsStatistic.add(1);
          deliveryLatencyStatistic.add(currentTimeMillis - doc.getLong("stimulus"));
          addToEncoder(doc);
        }

        List<ObjectId> sourceDocumentIDsToBecomeSent = new ArrayList<>();
        for (Document doc : unsentGroupedSourcesDocs) {
          List<ObjectId> duplicateDocumentIds = doc.getList("document_ids", ObjectId.class);
          sourceDocumentIDsToBecomeSent.add(duplicateDocumentIds.get(0));

          idsToMarkSent.addAll(duplicateDocumentIds);
        }

        FindIterable<Document> sourcesToBecomeSentDocs = anankeCollection
            .find(in("_id", sourceDocumentIDsToBecomeSent));

        currentTimeMillis = System.currentTimeMillis();
        for (Document doc : sourcesToBecomeSentDocs) {
          provenanceReadsStatistic.add(1);
          deliveryLatencyStatistic.add(currentTimeMillis - doc.getLong("stimulus"));
          addToEncoder(doc);
        }
        // mark all sent docs as sent
        anankeCollection.updateMany(in("_id", idsToMarkSent), setUnsentToSend);

        // --------- PART II: Acks

        long minimumSafeTs = anankeCollection.aggregate(asList(match(typeSafeTS), groupMinimum))
            .first().getLong("min");
        // retrieve all expired source tuples, deduplicate them
        // there will be one returned document per tuple uid, and the document contains a list of all documents
        // of all duplicates of the source tuple with that tuple uid
        AggregateIterable<Document> expiredGroupedSourceDocs = anankeCollection.aggregate(asList(
                match(and(typeSource, lessThan(minimumSafeTs), notExpired, sent)),
                group("$uid", Accumulators.addToSet("document_ids", "$_id"),
                    Accumulators.min("min_ts", "$ts"))));

        // create acks and sent them out
        ArrayList<ObjectId> acks = new ArrayList<>();

        for (Document groupedDoc : expiredGroupedSourceDocs) {
          // get the first of the duplicate documents
          provenanceReadsStatistic.add(1);
          AckDeliveryLatencyHelper.observeTimestamp(groupedDoc.getLong("min_ts"));
          encoder.ack(groupedDoc.getLong("_id"), DEFAULT_TIMESTAMP);
          acks.addAll(groupedDoc.getList("document_ids", ObjectId.class));
        }
        anankeCollection.updateMany(in("_id", acks), setToExpired);

        AckDeliveryLatencyHelper.commit();
        provenanceReadTimeStatistic.add(System.currentTimeMillis() - start);
        provenanceReadsStatistic.commit();
        deliveryLatencyStatistic.commit();
        encoder.commit();
      } catch (Exception exception) {
        LOG.error(exception.getMessage());
      }
    }

    private void addToEncoder(Document doc) {
      String type = doc.getString("type");
      switch (type) {
        case "sink":
          long tupleUID = doc.getLong("uid");
          encoder.sinkVertex(tupleUID, summarizeBytes(doc.get("data", Binary.class).getData()));
          encoder.ack(tupleUID, DEFAULT_TIMESTAMP);
          break;
        case "source":
          encoder.sourceVertex(doc.getLong("uid"),
              summarizeBytes(doc.get("data", Binary.class).getData()));
          break;
        case "edge":
          encoder.edge(doc.getLong("source_id"), doc.getLong("sink_id"), DEFAULT_TIMESTAMP);
          break;
      }
    }

    public void stop() {
      enabled = false;
    }
  }

}
