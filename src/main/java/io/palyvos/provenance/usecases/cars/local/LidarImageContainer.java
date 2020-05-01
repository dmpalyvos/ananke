package io.palyvos.provenance.usecases.cars.local;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class LidarImageContainer implements Serializable {

  public static class Annotation3D implements Serializable {

    public String labelClass;
    public double x;
    public double y;
    public double z;

    public Annotation3D() {
    }

    public Annotation3D(String labelClass, double x, double y, double z) {
      this.labelClass = labelClass;
      this.x = x;
      this.y = y;
      this.z = z;
    }
  }

  private static final byte IMAGE_SERIALIZATION_CODE = 0;
  private static final byte LIDAR_SERIALIZATION_CODE = 1;
  // annotations: key: object_ID, value: <object_name, x, y, z>
  private Map<String, Annotation3D> annotations = new HashMap<>();
  //    private byte[] image;
//    private byte[] lidar;
  private Long timestamp;
  private String payloadType;

  public LidarImageContainer(String payloadType, Long timestamp) {
    this.timestamp = timestamp;
    this.payloadType = payloadType;
  }

//    public void readImage(File file) throws FileNotFoundException, IOException {
//        try (InputStream input = new FileInputStream(file)) {
//            this.image = toByteArray(input);
//        }
//    }
//
//    public void readLiDAR(File file) throws FileNotFoundException, IOException {
//        try (InputStream input = new FileInputStream(file)) {
//            this.lidar = toByteArray(input);
//        }
//    }

  public void createAnnotations(File file) throws FileNotFoundException, IOException {
    try (Scanner sc = new Scanner(file)) {
      while (sc.hasNext()) {
        String line = sc.next();
        String[] tokens = line.split(",");
        String trackID = tokens[0];
        String labelClass = tokens[1];
        double x = Double.parseDouble(tokens[2]);
        double y = Double.parseDouble(tokens[3]);
        double z = Double.parseDouble(tokens[4]);
        this.annotations.put(trackID, new Annotation3D(labelClass, x, y, z));
      }
    }
  }

  public Map<String, Annotation3D> getAnnotations() {
    return annotations;
  }

//    public byte[] getImage() {
//        return image;
//    }
//
//    public byte[] getLidar() {
//        return lidar;
//    }

  public long getTimestamp() {
    return timestamp;
  }

  public String getPayloadType() {
    return payloadType;
  }

  public static class KryoSerializer extends Serializer<LidarImageContainer> implements
      Serializable {

    @Override
    public void write(Kryo kryo, Output output, LidarImageContainer object) {
      output.writeString(object.payloadType);
      output.writeLong(object.timestamp);
      kryo.writeObject(output, object.annotations);
//            if (object.image != null) {
//                output.writeByte(IMAGE_SERIALIZATION_CODE);
//                output.writeInt(object.image.length);
//                output.writeBytes(object.image);
//            }
//            else if (object.lidar != null) {
//                output.writeByte(LIDAR_SERIALIZATION_CODE);
//                output.writeInt(object.lidar.length);
//                output.writeBytes(object.lidar);
//                return;
//            }
    }

    @Override
    public LidarImageContainer read(Kryo kryo, Input input, Class<LidarImageContainer> type) {
      LidarImageContainer object = new LidarImageContainer(input.readString(), input.readLong());
      object.annotations = kryo.readObject(input, HashMap.class);
//            final byte serializationCode = input.readByte();
//            if (serializationCode == IMAGE_SERIALIZATION_CODE) {
//                object.image = input.readBytes(input.readInt());
//            }
//            else if (serializationCode == LIDAR_SERIALIZATION_CODE) {
//                object.lidar = input.readBytes(input.readInt());
//            }
//            else {
//                throw new IllegalStateException(String.format("Unknown serialization code: %d", serializationCode));
//            }
      return object;
    }
  }

  @Override
  public String toString() {
    return String.format("LIC(%d,%s,%d)", timestamp, payloadType, annotations.size());
  }
}



