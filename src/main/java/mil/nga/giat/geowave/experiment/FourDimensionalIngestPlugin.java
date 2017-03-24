package mil.nga.giat.geowave.experiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.experiment.avro.ExperimentalParams;

public class FourDimensionalIngestPlugin implements
		IngestFromHdfsPlugin<ExperimentalParams, FourDimensionalData>,
		AvroFormatPlugin<ExperimentalParams, FourDimensionalData>,
		LocalFileIngestPlugin<FourDimensionalData>
{
	private static Random rand = new Random(
			1000);

	@Override
	public PrimaryIndex[] getRequiredIndices() {
		return null;
	}

	@Override
	public Schema getAvroSchema() {
		return ExperimentalParams.SCHEMA$;
	}

	@Override
	public ExperimentalParams[] toAvroObjects(
			final File file ) {
		try {
			final BufferedReader r = new BufferedReader(
					new FileReader(
							file));
			final String numberOfAvro = r.readLine();
			final String recordsPerAvro = r.readLine();
			final long recordsPerAvroLong = Long.parseLong(recordsPerAvro);
			final ExperimentalParams[] params = new ExperimentalParams[Integer.parseInt(numberOfAvro)];
			for (int i = 0; i < params.length; i++) {
				params[i] = new ExperimentalParams(
						recordsPerAvroLong,
						(long) i);
			}
			r.close();
			return params;
		}
		catch (final FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (final IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {
			"conf"
		};
	}

	@Override
	public void init(
			final File baseDirectory ) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean supportsFile(
			final File file ) {
		return file.getName().endsWith(
				".conf");
	}

	@Override
	public IngestPluginBase<ExperimentalParams, FourDimensionalData> getIngestWithAvroPlugin() {
		return new IngestPlugin();
	}

	@Override
	public boolean isUseReducerPreferred() {
		return false;
	}

	@Override
	public IngestWithMapper<ExperimentalParams, FourDimensionalData> ingestWithMapper() {
		return new IngestPlugin();
	}

	@Override
	public IngestWithReducer<ExperimentalParams, ?, ?, FourDimensionalData> ingestWithReducer() {
		return null;
	}

	public static class IngestPlugin implements
			IngestPluginBase<ExperimentalParams, FourDimensionalData>,
			IngestWithMapper<ExperimentalParams, FourDimensionalData>
	{

		@Override
		public WritableDataAdapter<FourDimensionalData>[] getDataAdapters(
				final String globalVisibility ) {
			return new WritableDataAdapter[] {
				new FourDimensionalDataAdapter()
			};
		}

		@Override
		public Class<? extends CommonIndexValue>[] getSupportedIndexableTypes() {
			return new Class[] {
				XDimension.class,
				YDimension.class,
				ZDimension.class,
				TDimension.class
			};
		}

		@Override
		public CloseableIterator<GeoWaveData<FourDimensionalData>> toGeoWaveData(
				final ExperimentalParams input,
				final Collection<ByteArrayId> primaryIndexIds,
				final String globalVisibility ) {
			final long numData = input.getNumData();
			return new CloseableIterator<GeoWaveData<FourDimensionalData>>() {
				long i = 0;

				@Override
				public boolean hasNext() {
					return i < numData;
				}

				@Override
				public GeoWaveData<FourDimensionalData> next() {
					final byte[] value = new byte[500];
					rand.nextBytes(value);
					return new GeoWaveData<FourDimensionalData>(
							FourDimensionalDataAdapter.ADAPTER_ID,
							primaryIndexIds,
							new FourDimensionalData(
									new ByteArrayId(
											Long.toString(input.getId()) + "_" + Long.toString(i++)),
									value,
									rand.nextDouble(),
									rand.nextDouble(),
									rand.nextDouble(),
									rand.nextDouble()));
				}

				@Override
				public void close()
						throws IOException {

				}

			};
		}

		@Override
		public byte[] toBinary() {
			return new byte[] {};
		}

		@Override
		public void fromBinary(
				byte[] bytes ) {}

	}

	@Override
	public CloseableIterator<GeoWaveData<FourDimensionalData>> toGeoWaveData(
			final File file,
			final Collection<ByteArrayId> primaryIndexIds,
			final String globalVisibility ) {
		final ExperimentalParams[] p = toAvroObjects(file);
		final List<GeoWaveData<FourDimensionalData>> list = new ArrayList<>();
		for (final ExperimentalParams input : p) {
			list.addAll((Collection) Arrays.asList(Iterators.toArray(
					new IngestPlugin().toGeoWaveData(
							input,
							primaryIndexIds,
							globalVisibility),
					GeoWaveData.class)));
		}
		return new CloseableIterator.Wrapper<GeoWaveData<FourDimensionalData>>(
				list.iterator());
	}

	@Override
	public WritableDataAdapter<FourDimensionalData>[] getDataAdapters(
			final String globalVisibility ) {
		return new WritableDataAdapter[] {
			new FourDimensionalDataAdapter()
		};
	}

	@Override
	public Class<? extends CommonIndexValue>[] getSupportedIndexableTypes() {
		return new Class[] {
			XDimension.class,
			YDimension.class,
			ZDimension.class,
			TDimension.class
		};
	}
}
