package mil.nga.giat.geowave.experiment;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class FourDimensionalDataAdapter extends
		AbstractDataAdapter<FourDimensionalData>
{
	protected static final ByteArrayId ADAPTER_ID = new ByteArrayId(
			"4D");
	protected static final ByteArrayId X_ID = new ByteArrayId(
			"X");
	protected static final ByteArrayId Y_ID = new ByteArrayId(
			"Y");
	protected static final ByteArrayId Z_ID = new ByteArrayId(
			"Z");
	protected static final ByteArrayId T_ID = new ByteArrayId(
			"T");
	private Random random = new Random(
			1000L);

	@Override
	public ByteArrayId getAdapterId() {
		return ADAPTER_ID;
	}

	@Override
	public boolean isSupported(
			FourDimensionalData entry ) {
		return true;
	}

	@Override
	public ByteArrayId getDataId(
			FourDimensionalData entry ) {
		return entry.getDataId();
	}

	@Override
	public int getPositionOfOrderedField(
			CommonIndexModel model,
			ByteArrayId fieldId ) {
		switch (fieldId.getString()) {
			case "X":
				return 0;
			case "Y":
				return 1;
			case "Z":
				return 2;
			case "T":
				return 3;
		}
		return 0;
	}

	@Override
	public ByteArrayId getFieldIdForPosition(
			CommonIndexModel model,
			int position ) {
		switch (position) {
			case 0:
				return X_ID;
			case 1:
				return Y_ID;
			case 2:
				return Z_ID;
			case 3:
				return T_ID;
		}
		return null;
	}

	@Override
	public FieldReader<Object> getReader(
			ByteArrayId fieldId ) {
		return (FieldReader) FieldUtils.getDefaultReaderForClass(Double.class);
	}

	@Override
	public FieldWriter<FourDimensionalData, Object> getWriter(
			ByteArrayId fieldId ) {
		return (FieldWriter) FieldUtils.getDefaultWriterForClass(Double.class);
	}

	@Override
	protected RowBuilder<FourDimensionalData, Object> newBuilder() {
		return new RowBuilder() {
			private Map<ByteArrayId, Double> valueMap = new HashMap<>();

			@Override
			public void setField(
					PersistentValue fieldValue ) {
				valueMap.put(
						fieldValue.getId(),
						(Double) fieldValue.getValue());
			}

			@Override
			public Object buildRow(
					ByteArrayId dataId ) {
				final byte[] value = new byte[500];
				random.nextBytes(value);
				return new FourDimensionalData(
						dataId,
						value,
						valueMap.get(X_ID),
						valueMap.get(Y_ID),
						valueMap.get(Z_ID),
						valueMap.get(T_ID));
			}

		};
	}

}
