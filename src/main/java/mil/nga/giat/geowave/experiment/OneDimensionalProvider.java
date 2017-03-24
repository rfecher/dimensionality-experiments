package mil.nga.giat.geowave.experiment;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeOptions;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeProviderSpi;

public class OneDimensionalProvider implements
		DimensionalityTypeProviderSpi
{
	protected static final ByteArrayId X_INDEX_ID = new ByteArrayId(
			"X");
	protected static final ByteArrayId Y_INDEX_ID = new ByteArrayId(
			"Y");
	protected static final ByteArrayId Z_INDEX_ID = new ByteArrayId(
			"Z");
	protected static final ByteArrayId T_INDEX_ID = new ByteArrayId(
			"T");

	protected static final ByteArrayId INDEX_ID = new ByteArrayId(
			"OneD_IDX");

	@Override
	public Class<? extends CommonIndexValue>[] getRequiredIndexTypes() {
		return new Class[] {
			XDimension.class,
			YDimension.class,
			ZDimension.class,
			TDimension.class
		};
	}

	@Override
	public String getDimensionalityTypeName() {
		return "1D";
	}

	@Override
	public String getDimensionalityTypeDescription() {
		return "1D Experimentation";
	}

	@Override
	public int getPriority() {
		return 0;
	}

	@Override
	public PrimaryIndex createPrimaryIndex() {
		return new CustomIdIndex(
				TieredSFCIndexFactory.createSingleTierStrategy(
						new NumericDimensionDefinition[] {
							new XDimension(),
						},
						new int[] {
							15,
							15,
							15
						},
						SFCType.HILBERT),
				new BasicIndexModel(
						new NumericDimensionField[] {
							new DoubleDimensionField(
									new XDimension(),
									X_INDEX_ID),
							new DoubleDimensionField(
									new YDimension(),
									Y_INDEX_ID),
							new DoubleDimensionField(
									new ZDimension(),
									Z_INDEX_ID),
							new DoubleDimensionField(
									new TDimension(),
									T_INDEX_ID)
						}),
				INDEX_ID);
	}

	@Override
	public DimensionalityTypeOptions getOptions() {
		return null;
	}
}
