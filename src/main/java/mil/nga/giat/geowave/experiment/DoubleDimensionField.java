package mil.nga.giat.geowave.experiment;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.data.NumericValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;

public class DoubleDimensionField implements
		NumericDimensionField<DoubleIndexedField>
{
	private NumericDimensionDefinition definition;
	private ByteArrayId fieldId;

	public DoubleDimensionField() {}

	public DoubleDimensionField(
			NumericDimensionDefinition definition,
			ByteArrayId fieldId ) {
		this.definition = definition;
		this.fieldId = fieldId;
	}

	public byte[] toBinary() {
		byte[] def = PersistenceUtils.toBinary(definition);
		byte[] fieldIdBytes = fieldId.getBytes();
		ByteBuffer buf = ByteBuffer.allocate(def.length + fieldIdBytes.length + 4);
		buf.putInt(def.length);
		buf.put(def);
		buf.put(fieldIdBytes);
		return buf.array();
	}

	public void fromBinary(
			byte[] bytes ) {
		ByteBuffer buf = ByteBuffer.wrap(bytes);
		byte[] def = new byte[buf.getInt()];
		buf.get(def);
		byte[] fieldIdBytes = new byte[bytes.length - def.length - 4];
		buf.get(fieldIdBytes);
		fieldId = new ByteArrayId(
				fieldIdBytes);
		definition = PersistenceUtils.fromBinary(
				def,
				NumericDimensionDefinition.class);
	}

	public double getRange() {
		return definition.getRange();
	}

	public double normalize(
			double value ) {
		return definition.normalize(value);
	}

	public double denormalize(
			double value ) {
		return definition.denormalize(value);
	}

	public BinRange[] getNormalizedRanges(
			NumericData range ) {
		return definition.getNormalizedRanges(range);
	}

	public NumericRange getDenormalizedRange(
			BinRange range ) {
		return definition.getDenormalizedRange(range);
	}

	public int getFixedBinIdSize() {
		return definition.getFixedBinIdSize();
	}

	public NumericRange getBounds() {
		return definition.getBounds();
	}

	public NumericData getFullRange() {
		return definition.getFullRange();
	}

	@Override
	public NumericData getNumericData(
			DoubleIndexedField dataElement ) {
		return new NumericValue(
				dataElement.getValue());
	}

	@Override
	public ByteArrayId getFieldId() {
		return fieldId;
	}

	@Override
	public FieldWriter<?, DoubleIndexedField> getWriter() {
		return new DoubleIndexAdapter.Writer();
	}

	@Override
	public FieldReader<DoubleIndexedField> getReader() {
		return new DoubleIndexAdapter.Reader();
	}

	@Override
	public NumericDimensionDefinition getBaseDefinition() {
		return definition;
	}
}
