package mil.nga.giat.geowave.experiment;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.field.base.DoubleSerializationProvider;

public class DoubleIndexAdapter {
	public static class Reader implements FieldReader<DoubleIndexedField> {
		private static final FieldReader<Double> reader = new DoubleSerializationProvider().getFieldReader();

		@Override
		public DoubleIndexedField readField(byte[] fieldData) {
			return new DoubleIndexedField(reader.readField(fieldData));
		}

	}

	public static class Writer implements FieldWriter<FourDimensionalData, DoubleIndexedField> {
		private static final FieldWriter<Object, Double> writer = new DoubleSerializationProvider().getFieldWriter();

		@Override
		public byte[] getVisibility(FourDimensionalData rowValue, ByteArrayId fieldId, DoubleIndexedField fieldValue) {
			return fieldValue.getVisibility();
		}

		@Override
		public byte[] writeField(DoubleIndexedField fieldValue) {
			return writer.writeField(fieldValue.getValue());
		}

	}
}
