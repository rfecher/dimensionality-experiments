package mil.nga.giat.geowave.experiment;

import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.experiment.avro.ExperimentalParams;

public class FourDimensionalDataFormat implements
		IngestFormatPluginProviderSpi<ExperimentalParams, FourDimensionalData>
{

	@Override
	public IngestFromHdfsPlugin<ExperimentalParams, FourDimensionalData> createIngestFromHdfsPlugin(
			IngestFormatOptionProvider options )
			throws UnsupportedOperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LocalFileIngestPlugin<FourDimensionalData> createLocalFileIngestPlugin(
			IngestFormatOptionProvider options )
			throws UnsupportedOperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getIngestFormatName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IngestFormatOptionProvider createOptionsInstances() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getIngestFormatDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AvroFormatPlugin<ExperimentalParams, FourDimensionalData> createAvroFormatPlugin(
			IngestFormatOptionProvider options )
			throws UnsupportedOperationException {
		return null;
	}

}
