package mil.nga.giat.geowave.experiment;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeOptions;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeProviderSpi;

public class FourDimensionalProvider implements
		DimensionalityTypeProviderSpi
{

	@Override
	public Class<? extends CommonIndexValue>[] getRequiredIndexTypes() {
		return null;
	}

	@Override
	public String getDimensionalityTypeName() {
		return "4D";
	}

	@Override
	public String getDimensionalityTypeDescription() {
		return "4D Experimentation";
	}

	@Override
	public int getPriority() {
		return 0;
	}

	@Override
	public PrimaryIndex createPrimaryIndex() {
		return new PrimaryIndex(
				TieredSFCIndexFactory.createSingleTierStrategy(
						new NumericDimensionDefinition[] {
							new XDimension(),
							new YDimension(),
							new ZDimension(),
							new TDimension(),
						},
						new int[] {
							15,
							15,
							15,
							15
						},
						SFCType.HILBERT),
				new BasicIndexModel(
						new NumericDimensionField[] {
							new DoubleDimensionField(
									new XDimension(),
									FourDimensionalDataAdapter.X_ID),
							new DoubleDimensionField(
									new YDimension(),
									FourDimensionalDataAdapter.Y_ID),
							new DoubleDimensionField(
									new ZDimension(),
									FourDimensionalDataAdapter.Z_ID),
							new DoubleDimensionField(
									new TDimension(),
									FourDimensionalDataAdapter.T_ID)
						}));
	}

	@Override
	public DimensionalityTypeOptions getOptions() {
		return null;
	}

}
