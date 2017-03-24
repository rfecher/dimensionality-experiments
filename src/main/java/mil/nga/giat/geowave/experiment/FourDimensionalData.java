package mil.nga.giat.geowave.experiment;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

public class FourDimensionalData implements
		CommonIndexValue
{
	private byte[] value;
	private byte[] visibility = new byte[] {};
	private ByteArrayId dataId;
	private double x;
	private double y;
	private double z;
	private double t;

	protected FourDimensionalData() {}

	public FourDimensionalData(
			ByteArrayId dataId,
			byte[] value,
			double x,
			double y,
			double z,
			double t ) {
		this.dataId = dataId;
		this.value = value;
		this.x = x;
		this.y = y;
		this.z = z;
		this.t = t;
	}

	public ByteArrayId getDataId() {
		return dataId;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(
			byte[] value ) {
		this.value = value;
	}

	public double getX() {
		return x;
	}

	public void setX(
			double x ) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(
			double y ) {
		this.y = y;
	}

	public double getZ() {
		return z;
	}

	public void setZ(
			double z ) {
		this.z = z;
	}

	public double getT() {
		return t;
	}

	public void setT(
			double t ) {
		this.t = t;
	}

	@Override
	public byte[] getVisibility() {
		return visibility;
	}

	@Override
	public void setVisibility(
			byte[] visibility ) {
		this.visibility = visibility;
	}

	@Override
	public boolean overlaps(
			NumericDimensionField[] field,
			NumericData[] rangeData ) {
		return true;
	}
}
