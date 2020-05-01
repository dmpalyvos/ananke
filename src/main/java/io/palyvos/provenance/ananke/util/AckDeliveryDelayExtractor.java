package io.palyvos.provenance.ananke.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;



public class AckDeliveryDelayExtractor {

  private static final int TYPE_COLUMN = 1;
  private static final int FLINK_TIMESTAMP_COLUMN = 0;
  private static final int DATA_TIMESTAMP_COLUMN = 2;
  private static final int CONTENT_START_COLUMN = 3;
  public static final String COLUMN_SEPARATOR = " ";

  public static void main(String[] args) throws IOException {

    String input = args[0];
    String output = args[1];

    final PrintWriter f0 = new PrintWriter(new FileWriter(output));
    final AckDeliveryDelayExtractor EX = new AckDeliveryDelayExtractor();
    final List<String> lines = EX.getLines(input);

    final HashMap<String, Long> vertexes = new HashMap<>();
    final HashMap<String, Long> vertexesTypes = new HashMap<>();
    final HashMap<String, Long> acks = new HashMap<>();

    long dummyTimestamp = 0;
    for (String l : lines) {
      if (EX.isVertexLine(l)) {
        String id = EX.getVertexId(l);
        if (vertexes.containsKey(id)) {
          System.err.println("duplicated vertex " + id);
        } else if (acks.containsKey(id)) {
          System.err.println("vertex " + id + " appears after ack");
        } else {
          vertexes.put(id, EX.getFlinkTimestamp(l));
          vertexesTypes.put(id, EX.getType(l));
          f0.println("V"+vertexesTypes.get(id) + "," + dummyTimestamp++ + "," + (EX.getFlinkTimestamp(l) - EX
              .getTimestamp(l)));
        }
      }
      else if (EX.isAckLine(l)) {
        String id = EX.getAckId(l);
        if (acks.containsKey(id)) {
          System.err.println("duplicated ack " + id);
        } else if (!vertexes.containsKey(id)) {
          System.err.println("vertex " + id + " appears after ack");
        } else {
          acks.put(id, EX.getTimestamp(l));
          f0.println("A"+vertexesTypes.get(id) + "," + dummyTimestamp++ + "," + (acks.get(id) - vertexes.get(id)));
        }
      }
      else if (EX.isEdgeLine(l)) {
        String id1 = EX.getEdgeId1(l);
        String id2 = EX.getEdgeId2(l);
        long edgeTimestamp = EX.getTimestamp(l);
        Long v1Timestamp = vertexes.get(id1);
        Long v2Timestamp = vertexes.get(id2);
        if (v1Timestamp == null || v2Timestamp == null) {
          System.err.println("Edge " + id1 + "," + id2 + " came before both vertices were provided");
          continue;
        }
        long latency = edgeTimestamp - Math.max(v1Timestamp, v2Timestamp);
        f0.println(EX.getType(l) + "," + dummyTimestamp++ + "," + latency);
      }
    }
    for (String id : vertexes.keySet()) {
      if (!acks.containsKey(id)) {
        System.err.println("vertex " + id + " was not acked!");
      }
    }

    f0.close();


  }

  private Long getType(String l) {
    return Long.valueOf(l.split(COLUMN_SEPARATOR)[TYPE_COLUMN]);
  }

  private long getTimestamp(String l) {
    return Long.valueOf(l.split(COLUMN_SEPARATOR)[DATA_TIMESTAMP_COLUMN]);
  }

  private long getFlinkTimestamp(String l) {
    return Long.valueOf(l.split(COLUMN_SEPARATOR)[FLINK_TIMESTAMP_COLUMN]);
  }

  List<String> getVertexIds(List<String> lines) {
    List<String> result = new LinkedList<String>();
    for (String line : lines) {
      if (isVertexLine(line)) {
        result.add(getVertexId(line));
      }
    }
    return result;
  }

  List<String> getAckIds(List<String> lines) {
    List<String> result = new LinkedList<String>();
    for (String line : lines) {
      if (isAckLine(line)) {
        result.add(getAckId(line));
      }
    }
    return result;
  }

  List<String> getEdgesJointId(List<String> lines) {
    List<String> result = new LinkedList<String>();
    for (String line : lines) {
      if (isEdgeLine(line)) {
        result.add(getEdgeId1(line) + getEdgeId2(line));
      }
    }
    return result;
  }

  String getVertexId(String line) {
    // return line.split("\\[")[1].split("\\]")[0];
    return line.split(COLUMN_SEPARATOR)[CONTENT_START_COLUMN + 2];
  }

  boolean isAckLine(String line) {
    return line.split(COLUMN_SEPARATOR)[CONTENT_START_COLUMN].equals("ACK");
  }


  boolean isVertexLine(String line) {
    try {
      return line.split(COLUMN_SEPARATOR)[CONTENT_START_COLUMN].equals("SINK")
          || line.split(COLUMN_SEPARATOR)[CONTENT_START_COLUMN].equals("SOURCE");
    }
    catch (Exception exception) {
      System.err.println(AckDeliveryDelayExtractor.class.getSimpleName() + ": Failed to parse output file");
      System.exit(1);
      return false;
    }
  }

  boolean isEdgeLine(String line) {
    return line.split(COLUMN_SEPARATOR)[CONTENT_START_COLUMN].equals("EDGE");
  }

  String getAckId(String line) {
    // return line.split("\\[")[1].split("\\]")[0];
    return line.split(COLUMN_SEPARATOR)[CONTENT_START_COLUMN + 2];
  }

  String getEdgeId1(String line) {
    // return line.split("->")[0].split("\\[")[1].split("\\]")[0];
    return line.split(COLUMN_SEPARATOR)[CONTENT_START_COLUMN + 2];
  }

  String getEdgeId2(String line) {
    // return line.split("->")[1].split("\\[")[1].split("\\]")[0];
    return line.split(COLUMN_SEPARATOR)[CONTENT_START_COLUMN + 4];
  }

  List<String> getLines(String file) {
    List<String> result = new LinkedList<String>();
    BufferedReader reader;
    try {
      reader = new BufferedReader(new FileReader(file));
      String line = reader.readLine();
      while (line != null) {
        result.add(line);
        line = reader.readLine();
      }
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return result;
  }

  List<String> getAllVertexConnectedToVertex(String vertexID, List<String> lines) {
    List<String> result = new LinkedList<String>();
    for (String line : lines) {
      if (isEdgeLine(line) && getEdgeId1(line).equals(vertexID)) {
        result.add(getEdgeId2(line));
      } else if (isEdgeLine(line) && getEdgeId2(line).equals(vertexID)) {
        result.add(getEdgeId1(line));
      }
    }
    return result;
  }

}
