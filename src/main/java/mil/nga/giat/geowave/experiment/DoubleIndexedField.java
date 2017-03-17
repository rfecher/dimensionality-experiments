package mil.nga.giat.geowave.experiment;

import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

public class DoubleIndexedField implements CommonIndexValue {

	private byte[] visibility = new byte[] {};
	private double value;
	public DoubleIndexedField(double value) {
		this.value = value;
	}

	public double getValue() {
		return value;
	}

	@Override
	public byte[] getVisibility() {
		return visibility;
	}

	@Override
	public void setVisibility(byte[] visibility) {
		this.visibility = visibility;
	}

	@Override
	public boolean overlaps(NumericDimensionField[] field, NumericData[] rangeData) {
		return true;
	}

}
