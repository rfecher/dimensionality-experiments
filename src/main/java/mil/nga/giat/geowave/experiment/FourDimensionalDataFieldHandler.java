package mil.nga.giat.geowave.experiment;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

public class FourDimensionalDataFieldHandler implements
		IndexFieldHandler<FourDimensionalData, DoubleIndexedField, Object>
{
	private ByteArrayId fieldId;

	public FourDimensionalDataFieldHandler(
			ByteArrayId fieldId ) {
		this.fieldId = fieldId;
	}

	@Override
	public ByteArrayId[] getNativeFieldIds() {
		return new ByteArrayId[] {
			fieldId
		};
	}

	@Override
	public DoubleIndexedField toIndexValue(
			FourDimensionalData row ) {
		switch (fieldId.getString()) {
			case "X":
				return new DoubleIndexedField(
						row.getX());
			case "Y":
				return new DoubleIndexedField(
						row.getY());
			case "Z":
				return new DoubleIndexedField(
						row.getZ());
			case "T":
				return new DoubleIndexedField(
						row.getT());
		}
		return null;
	}

	@Override
	public PersistentValue<Object>[] toNativeValues(
			DoubleIndexedField indexValue ) {
		return new PersistentValue[] {
			new PersistentValue<Object>(
					fieldId,
					indexValue.getValue())
		};
	}

}
