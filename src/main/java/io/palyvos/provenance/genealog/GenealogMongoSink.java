package io.palyvos.provenance.genealog;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Updates.set;
import static java.util.Arrays.asList;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.UpdateOptions;
import io.palyvos.provenance.util.AckDeliveryLatencyHelper;
import io.palyvos.provenance.util.AnankeMongoDBQueryRunner;
import io.palyvos.provenance.util.AvgStat;
import io.palyvos.provenance.util.CountStat;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.SimpleBackoff;
import io.palyvos.provenance.util.TimestampConverter;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink that WRITES {@link GenealogTuple} stream (including provenance) to MongoDB and records
 * latency.
 */
public class GenealogMongoSink<T extends GenealogTuple> extends RichSinkFunction<T> implements
    Consumer<Watermark> {

  public static MongoClient MONGO_CLIENT;
  private static Logger LOG = LoggerFactory.getLogger(GenealogMongoSink.class);

  public static final String DATABASE_NAME = "ananke";
  public static final String COLLECTION_NAME = "ananke";
  public static final String MONGO_URL = "mongodb://localhost:27017";

  protected final ExperimentSettings settings;
  private final GenealogGraphTraverser genealogGraphTraverser;
  private final String name;
  private int taskIndex;
  private final long delayConstantMillis;
  private final TimestampConverter timestampConverter;
  private transient MongoDatabase database;
  private transient AnankeMongoDBQueryRunner queryRunner;
  private transient KryoSerializer<TimestampedUIDTuple> serializer;
  private transient DataOutputSerializer outputSerializer;

  private transient AvgStat provenanceWriteTimeStatistic;
  private transient CountStat provenanceWritesStatistic;

  private static final Bson typeSafeTS = eq("type", "safe_ts");
  private static final Bson groupMinimum = group(null, Accumulators.min("min", "$ts"));


  public GenealogMongoSink(String name, long delayConstantMillis, ExperimentSettings settings,
      TimestampConverter timestampConverter) {
    Validate.notBlank(name, "name");
    Validate.notNull(settings, "settings");
    Validate.notNull(timestampConverter, "timestampConverter");
    this.name = name;
    this.delayConstantMillis = delayConstantMillis;
    this.timestampConverter = timestampConverter;
    this.settings = settings;
    this.genealogGraphTraverser = new GenealogGraphTraverser(
        settings.aggregateStrategySupplier().get());
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    taskIndex = getRuntimeContext().getIndexOfThisSubtask();
    serializer = new KryoSerializer<>(TimestampedUIDTuple.class,
        getRuntimeContext().getExecutionConfig());
    outputSerializer = new DataOutputSerializer(AbstractSqlSink.WRITE_BUFFER_START_SIZE);
    provenanceWriteTimeStatistic = new AvgStat(settings
        .provenanceWriteTimeFile(taskIndex, name),
        settings.autoFlush());
    provenanceWritesStatistic = new CountStat(
        settings.provenanceWritesFile(getRuntimeContext().getIndexOfThisSubtask(), name),
        settings.autoFlush());
    synchronized (GenealogMongoSink.class) {
      if (MONGO_CLIENT == null) {
        MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(MONGO_URL))
            .retryWrites(true)
            .build();
        MONGO_CLIENT = MongoClients.create(mongoClientSettings);
      }
    }
    database = MONGO_CLIENT.getDatabase(DATABASE_NAME);
    queryRunner = new AnankeMongoDBQueryRunner(settings);
    queryRunner.start();
    super.open(parameters);
  }

  @Override
  public void close() throws Exception {
    super.close();
    queryRunner.stop();
    provenanceWritesStatistic.close();
    provenanceWriteTimeStatistic.close();
  }

  @Override
  public void invoke(T sinkTuple, Context context) {
    SimpleBackoff backoff = new SimpleBackoff();
    boolean success = false;
    Set<TimestampedUIDTuple> provenance = genealogGraphTraverser.getProvenance(sinkTuple);
    final long start = System.currentTimeMillis();
    MongoCollection<Document> anankeCollection = database.getCollection(COLLECTION_NAME);
    List<Document> insertions = new ArrayList<>();
    insertions.add(sinkToDocument(sinkTuple));

    for (TimestampedUIDTuple sourceTuple : provenance) {
      insertions.add(sourceToDocument(sourceTuple));
      insertions.add(edgeToDocument(sourceTuple, sinkTuple));
    }
    while (!success) {
      try {
        anankeCollection.insertMany(insertions);
        success = true;
        provenanceWritesStatistic.increase(2 + 3 * provenance.size());
        provenanceWriteTimeStatistic.add(System.currentTimeMillis() - start);
      } catch (Exception e) {
        LOG.warn(e.getMessage());
        backoff.backoff();
      }
    }
  }


  @Override
  public void accept(Watermark watermark) {
    SimpleBackoff backoff = new SimpleBackoff();
    final long start = System.currentTimeMillis();
    boolean success = false;

    MongoCollection<Document> anankeCollection = database.getCollection(COLLECTION_NAME);
    Bson cond1 = eq("type", "safe_ts");
    Bson cond2 = eq("sink_id", name);
    Bson updateSafeTSFilter = and(cond1, cond2);
    Bson update = set("ts", watermark.getTimestamp() - delayConstantMillis);

    while (!success) {
      try {
        anankeCollection.updateOne(updateSafeTSFilter, update, new UpdateOptions().upsert(true));
        // fetch the minimum safe timestamp
        long minimumSafeTs = anankeCollection.aggregate(asList(match(typeSafeTS), groupMinimum))
            .first().getLong("min");
        AckDeliveryLatencyHelper.updateExpired(minimumSafeTs, start);
        success = true;
        provenanceWriteTimeStatistic.add(System.currentTimeMillis() - start);
      } catch (Exception e) {
        LOG.warn(e.getMessage());
        backoff.backoff();
      }
    }

  }

  Document asDocument(TimestampedUIDTuple tuple) {
    Document document = new Document("uid", tuple.getUID());
    document.append("ts", timestampConverter.apply(tuple.getTimestamp()));
    document.append("data", AbstractSqlSink.serialize(tuple, serializer, outputSerializer));
    return document;
  }

  Document sinkToDocument(T tuple) {
    Document document = asDocument(tuple);
    document.append("type", "sink");
    document.append("sent", 0);
    document.append("stimulus", System.currentTimeMillis());
    return document;
  }

  Document sourceToDocument(TimestampedUIDTuple tuple) {
    Document document = asDocument(tuple);
    document.append("type", "source");
    document.append("sent", 0);
    document.append("expired", 0);
    document.append("stimulus", System.currentTimeMillis());
    return document;
  }

  Document edgeToDocument(TimestampedUIDTuple sourceTuple, T sinkTuple) {
    Document document = new Document();
    document.append("sink_id", sinkTuple.getUID());
    document.append("type", "edge");
    document.append("source_id", sourceTuple.getUID());
    document.append("sent", 0);
    document.append("stimulus", System.currentTimeMillis());
    return document;
  }
}

