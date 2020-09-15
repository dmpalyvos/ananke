package io.palyvos.provenance.util;


import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.Accumulators;
import io.palyvos.provenance.ananke.output.NoOpProvenanceGraphEncoder;
import io.palyvos.provenance.ananke.output.ProvenanceGraphEncoder;
import io.palyvos.provenance.ananke.output.TimestampedFileProvenanceGraphEncoder;
import io.palyvos.provenance.genealog.GenealogMongoSink;
import org.apache.commons.lang3.Validate;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.Doc;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.set;
import static io.palyvos.provenance.util.AbstractDatabaseQueryRunner.DEFAULT_TIMESTAMP;
import static java.util.Arrays.asList;


public class AnankeMongoDBQueryRunnerDebug {

  public static void main(String[] args) {

      Runner runner = new Runner();
      runner.run();

  }



  private static class Runner implements Runnable {

//    Bsons that are recycled ********************************************
    private static final Bson unsent = eq("sent", 0);
    private static final Bson typeSink = eq("type", "sink");
    private static final Bson typeSource = eq("type", "source");
    private static final Bson typeEdge = eq("type", "edge");
    private static final Bson sinkOrSourceOrEdge = or(typeSink, typeSource, typeEdge);
    private static final Bson unsentNodesAndEdges = and(unsent, sinkOrSourceOrEdge);

    private static final Bson setUnsentToSend = set("sent", true);

    private static final Bson typeSafeTS = eq("type", "safe_ts");
    private static final Bson matchSafeTS = match(typeSafeTS);
    private static final Bson groupMinimum = group(null, Accumulators.min("min", "$ts"));
    private static final List<Bson> findMinimum = asList(matchSafeTS, groupMinimum);

    private Bson lessThan(long timestamp) { return lt("ts", timestamp); }
    private static final Bson notExpired = eq("expired", false);
    private static final Bson sent = eq("sent", 1);
    private Bson findAcks(long timestamp) { return and(typeSource, lessThan(timestamp), sent, notExpired); }
    private static final Bson setToExpired = set("expired", true);
    // ************************************************************************

    public static final int DEFAULT_TIMESTAMP = -1;
    private transient MongoDatabase database;
    private volatile boolean enabled = true;

    public Runner() {
      ConnectionString connString = new ConnectionString(GenealogMongoSink.MONGO_URL);
      MongoClientSettings settings = MongoClientSettings.builder()
              .applyConnectionString(connString)
              .retryWrites(true)
              .build();
      MongoClient mongoClient = MongoClients.create(settings);
      database = mongoClient.getDatabase(GenealogMongoSink.DATABASE_NAME);
    }


    @Override
    public void run() {
        try {
            fetchAndUpdate();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//      fetchAndUpdate();
//      break;
    }


    public void fetchAndUpdate() throws InterruptedException {

        Thread.sleep(1000);
        MongoCollection<Document> anankeCollection = database
                .getCollection(GenealogMongoSink.COLLECTION_NAME);
        System.out.println("Connected to ANANKE MONGO DATABASE");

        // retrieve all expired source tuples, deduplicate them
        // there will be one returned document per tuple uid, and the document contains a list of all documents
        // of all duplicates of the source tuple with that tuple uid
        AggregateIterable<Document> expiredSourceDocs = anankeCollection.aggregate(asList(
                match(and(typeSource,sent)),
                group("$uid", Accumulators.addToSet("document_ids", "$_id"))));

//        AggregateIterable<Document> unsentDeduplicatedSourcesDocs = anankeCollection
//            .aggregate(asList(
//                match(typeSource),
//                group("$uid", Accumulators.addToSet("document_ids", "$_id"),
//                    Accumulators.max("combined_sent", "$sent"))
////                match(eq("combined_sent", 1))
//                )
//            );

        // create acks and sent them out
        int counter = 0;
        for (Document groupedDoc : expiredSourceDocs) {
                counter++;
        }
        System.out.println(counter);



//        // mark sources that were now sent out as acks as "expired" = 2
//        ArrayList<ObjectId> listSourceDocsToMarkExpired2 = new ArrayList<>();
//        for (Document groupedDoc : expiredSourceDocs) {
//            listSourceDocsToMarkExpired2.addAll(groupedDoc.getList("document_ids", ObjectId.class));
//        }

    }


    private boolean trySleep() {
      return true;
    }

    private Bson processSourceAcksAndReturnIDs(FindIterable<Document> documents) {
      ArrayList<ObjectId> ids = new ArrayList();
      final long currentTimeMillis = System.currentTimeMillis();
      for (Document doc : documents) {
        ids.add(doc.getObjectId("_id"));
      }
      return in("_id", ids);
    }

    private Bson processAndReturnIDs(FindIterable<Document> documents)  {
      ArrayList<ObjectId> ids = new ArrayList();
      final long currentTimeMillis = System.currentTimeMillis();
      for (Document doc : documents) {
        ids.add(doc.getObjectId("_id"));
        addToEncoder(doc);
      }
        System.out.println("---> found " + ids.size() + " elements in mongo");
      return in("_id", ids);
    }

    private void addToEncoder(Document doc) {
      String type = doc.getString("type");
      if (type.equals("sink")) {
        long tupleUID = doc.getLong("uid");
      } else if (type.equals("source")) {
      } else if (type.equals("edge")) {
      }
    }

    protected final String summarizeBytes(byte[] bytes) {
      return String.format("%s (%d bytes)", bytes.toString(), bytes.length);
    }

    public void stop() {
      enabled = false;
    }
  }

}
