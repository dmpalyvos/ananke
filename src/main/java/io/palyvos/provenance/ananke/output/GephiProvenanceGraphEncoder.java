package io.palyvos.provenance.ananke.output;

import com.google.gson.Gson;
import io.palyvos.provenance.util.IncreasingUIDGenerator;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GephiProvenanceGraphEncoder implements ProvenanceGraphEncoder, Serializable {

  private static final Set<Class<?>> ACCEPTED_DATA_CLASSES = new HashSet<>();

  static {
    ACCEPTED_DATA_CLASSES.add(Boolean.class);
    ACCEPTED_DATA_CLASSES.add(Character.class);
    ACCEPTED_DATA_CLASSES.add(Byte.class);
    ACCEPTED_DATA_CLASSES.add(Short.class);
    ACCEPTED_DATA_CLASSES.add(Integer.class);
    ACCEPTED_DATA_CLASSES.add(Long.class);
    ACCEPTED_DATA_CLASSES.add(Float.class);
    ACCEPTED_DATA_CLASSES.add(Double.class);
    ACCEPTED_DATA_CLASSES.add(String.class);
  }

  private static Logger LOG = LoggerFactory.getLogger(GephiProvenanceGraphEncoder.class);
  private static final String ADD_NODE_COMMAND = "an";
  private static final String ADD_EDGE_COMMAND = "ae";
  private static final String CHANGE_NODE_COMMAND = "cn";
  private static final Gson gson = new Gson();
  private static final int SOURCE_TUPLE_TYPE = 2;
  private static final int SINK_TUPLE_TYPE = 3;
  private final String gephiWorkspace;
  private final Set<String> existingNodes = new HashSet<>();

  static class DetailedUID {

    final String componentUID;
    final String tupleUID;

    public DetailedUID(String componentUID, String tupleUID) {
      this.componentUID = componentUID;
      this.tupleUID = tupleUID;
    }

    public static DetailedUID ofLong(long uid) {
      String convertedUID = IncreasingUIDGenerator.asString(uid);
      String[] uidParts = convertedUID.split(":");
      return new DetailedUID("C" + uidParts[0], uidParts[0] + uidParts[1]);
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this)
          .append("componentUID", componentUID)
          .append("tupleUID", tupleUID)
          .toString();
    }
  }

  static class GraphEdge {

    final String source;
    final String target;
    final boolean directed;

    GraphEdge(String source, String target, boolean directed) {
      this.source = source;
      this.target = target;
      this.directed = directed;
    }
  }

  public GephiProvenanceGraphEncoder(String gephiWorkspace) {
    Validate.notEmpty(gephiWorkspace);
    this.gephiWorkspace = gephiWorkspace;
  }

  @Override
  public void sourceVertex(TimestampedUIDTuple tuple, long streamTimestamp, long dataTimestamp) {
    DetailedUID uid = DetailedUID.ofLong(tuple.getUID());
    addNode(
        uid,
        uid.tupleUID,
        tuple,
        SOURCE_TUPLE_TYPE,
        false, tuple.getTimestamp());
  }

  @Override
  public void sinkVertex(TimestampedUIDTuple tuple, long streamTimestamp, long dataTimestamp) {
    DetailedUID uid = DetailedUID.ofLong(tuple.getUID());
    addNode(
        uid, uid.tupleUID, tuple, SINK_TUPLE_TYPE,
        false, tuple.getTimestamp());
  }

  @Override
  public void edge(long sourceUID, long sinkUID, long timestamp) {
    addEdge(
        UUID.randomUUID().toString(),
        DetailedUID.ofLong(sourceUID).tupleUID,
        DetailedUID.ofLong(sinkUID).tupleUID,
        false);
  }

  @Override
  public void ack(long uid, long timestamp) {
    changeNode(DetailedUID.ofLong(uid).tupleUID, null, true);
  }

  @Override
  public void close() {
  }

  private void addNode(
      DetailedUID uid, String label, Object data, Integer type, boolean done, long timestamp) {
    if (existingNodes.contains(uid)) {
      return;
    }
    Map<String, Map<String, Object>> nodes = new HashMap<>();
    nodes.put(uid.tupleUID, newGraphNode(label, type, data, done, timestamp, uid));
    addGraphEntities(gson, ADD_NODE_COMMAND, nodes);
    existingNodes.add(uid.tupleUID);
  }

  private void changeNode(String id, Integer size, boolean done) {
    Map<String, Map<String, Object>> nodes = new HashMap<>();
    nodes.put(id, newGraphNode(null, null, null, done, null, null));
    addGraphEntities(gson, CHANGE_NODE_COMMAND, nodes);
  }

  private void addEdge(String id, String source, String target, boolean directed) {
    Map<String, GraphEdge> nodes = new HashMap<>();
    nodes.put(id, new GraphEdge(source, target, directed));
    addGraphEntities(gson, ADD_EDGE_COMMAND, nodes);
  }

  private void addGraphEntities(Gson gson, String command, Map<String, ?> entities) {
    Map<String, Map<String, ?>> requests = new HashMap<>();
    requests.put(command, entities);
    String requestsJson = gson.toJson(requests);
    StringEntity requestEntity = new StringEntity(requestsJson, ContentType.APPLICATION_JSON);
    HttpPost postMethod =
        new HttpPost(
            String.format("http://localhost:8080/%s?operation=updateGraph", gephiWorkspace));
    postMethod.setEntity(requestEntity);
    HttpClient httpClient = HttpClients.createDefault();
    try {
      HttpResponse rawResponse = httpClient.execute(postMethod);
      if (rawResponse.getStatusLine().getStatusCode() != 200) {
        System.out.println("Failed to POST node to Gephi");
      }
    } catch (Exception exception) {
      exception.printStackTrace();
    }
  }

  private static Map<String, Object> newGraphNode(String label, Integer type, Object data,
      boolean done, Long timestamp,
      DetailedUID uid) {
    final Map<String, Object> node = new HashMap<>();
    node.put("label", label);
    for (Map.Entry<String, Object> dataAttribute : objectToMap(data).entrySet()) {
      node.put(String.format("data.%s", dataAttribute.getKey()), dataAttribute.getValue());
    }
    node.put("type", type);
    node.put("done", String.valueOf(done));
    node.put("timestamp", timestamp);
    for (Map.Entry<String, Object> dataAttribute : objectToMap(uid).entrySet()) {
      node.put(String.format("uid.%s", dataAttribute.getKey()), dataAttribute.getValue());
    }
    return node;
  }

  private static Map<String, Object> objectToMap(Object data) {
    Map<String, Object> objectMap = new HashMap<>();
    if (data == null) {
      return objectMap;
    }
    final Class<?> clazz = data.getClass();
    if (clazz.isPrimitive() || clazz.isAssignableFrom(String.class)) {
      objectMap.put("value", data);
      return objectMap;
    }
    final List<Field> allFields = getAllFields(new ArrayList<>(), clazz);
    for (Field field : allFields) {
      field.setAccessible(true);
      addFieldToObjectMap(field, data, objectMap);
    }
    return objectMap;
  }

  public static List<Field> getAllFields(List<Field> fields, Class<?> type) {
    fields.addAll(Arrays.asList(type.getDeclaredFields()));
    if (type.getSuperclass() != null) {
      getAllFields(fields, type.getSuperclass());
    }
    return fields;
  }

  private static void addFieldToObjectMap(Field field, Object data, Map<String, Object> objectMap) {
    try {
      final Object value = field.get(data);
      if (value != null && ACCEPTED_DATA_CLASSES.contains(value.getClass())) {
        objectMap.put(field.getName(), value);
      }
    } catch (IllegalAccessException e) {
      LOG.warn("Failed to access field!", e);
    }
  }

}
