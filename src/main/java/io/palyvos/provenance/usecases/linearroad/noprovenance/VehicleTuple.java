package io.palyvos.provenance.usecases.linearroad.noprovenance;


import io.palyvos.provenance.util.BaseTuple;
import java.util.Objects;

public class VehicleTuple extends BaseTuple {

	private int vid;
	private int reports;
	private int latestXWay;
	private int latestLane;
	private int latestDir;
	private int latestSeg;
	private int latestPos;
	private boolean uniquePosition;


	public VehicleTuple(long timestamp, int vid, int reports, int xway, int lane,
			int dir, int seg, int pos, boolean uniquePosition, long stimulus) {
	  super(timestamp, xway + "," + lane + "," + dir + "," + seg + "," + pos, stimulus);
		this.vid = vid;
		this.reports = reports;
		this.latestXWay = xway;
		this.latestLane = lane;
		this.latestDir = dir;
		this.latestSeg = seg;
		this.latestPos = pos;
		this.uniquePosition = uniquePosition;
	}

  public int getVid() {
    return vid;
  }

  public int getReports() {
    return reports;
  }

  public int getLatestXWay() {
    return latestXWay;
  }

  public int getLatestLane() {
    return latestLane;
  }

  public int getLatestDir() {
    return latestDir;
  }

  public int getLatestSeg() {
    return latestSeg;
  }

  public int getLatestPos() {
    return latestPos;
  }

  public boolean isUniquePosition() {
    return uniquePosition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    VehicleTuple that = (VehicleTuple) o;
    return vid == that.vid &&
        reports == that.reports &&
        latestXWay == that.latestXWay &&
        latestLane == that.latestLane &&
        latestDir == that.latestDir &&
        latestSeg == that.latestSeg &&
        latestPos == that.latestPos &&
        uniquePosition == that.uniquePosition;
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(super.hashCode(), vid, reports, latestXWay, latestLane, latestDir, latestSeg,
            latestPos,
            uniquePosition);
  }

  @Override
  public String toString() {
    return getTimestamp() + "," + vid + "," + reports + "," + latestXWay
        + "," + latestLane + "," + latestDir + "," + latestSeg + ","
        + latestPos + "," + uniquePosition;
  }
}
