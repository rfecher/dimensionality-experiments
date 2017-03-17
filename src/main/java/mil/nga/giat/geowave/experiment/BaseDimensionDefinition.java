package mil.nga.giat.geowave.experiment;

import mil.nga.giat.geowave.core.index.dimension.BasicDimensionDefinition;

public class BaseDimensionDefinition extends
		BasicDimensionDefinition
{

	public BaseDimensionDefinition() {
		super(
				0,
				1);
	}

	public BaseDimensionDefinition(
			final double min,
			final double max ) {
		super(
				min,
				max);
	}

}
