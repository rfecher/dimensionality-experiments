package mil.nga.giat.geowave.experiment;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.time.StopWatch;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintData;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintSet;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;

public class QueryMetrics
{
	private static Random rand = new Random(
			1000);

	public static void main(
			final String[] args )
			throws IOException {
		HBaseOptions options = new HBaseOptions();
		options.setServerSideDisabled(true);
		final HBaseDataStore ds = new HBaseDataStore(
				new BasicHBaseOperations(
						args[0],
						args[1]),
				options);
		final Integer samples = Integer.parseInt(args[2]);
		System.err.println(Statistics.getCSVHeader());
		for (double pct = Double.parseDouble(args[3]); pct < Double.parseDouble(args[4]); pct *= 2) {
			final double[] x = getRange(pct);
			final double[] y = getRange(pct);
			final double[] z = getRange(pct);
			final double[] t = getRange(pct);
			queryIndex(
					x,
					y,
					z,
					t,
					FourDimensionalProvider.INDEX_ID,
					samples,
					ds,
					pct);
			queryIndex(
					x,
					y,
					z,
					t,
					ThreeDimensionalProvider.INDEX_ID,
					samples,
					ds,
					pct);
			queryIndex(
					x,
					y,
					z,
					t,
					TwoDimensionalProvider.INDEX_ID,
					samples,
					ds,
					pct);
			queryIndex(
					x,
					y,
					z,
					t,
					OneDimensionalProvider.INDEX_ID,
					samples,
					ds,
					pct);
		}
	}

	private static double[] getRange(
			final double pct ) {
		double start = rand.nextDouble();
		// while (start > (1 - pct)) {
		// start = rand.nextDouble();
		// }
		final double end = start + pct;
		return new double[] {
			start,
			end
		};
	}

	private static void queryIndex(
			final double[] x,
			final double[] y,
			final double[] z,
			final double[] t,
			final ByteArrayId indexId,
			final int samples,
			final HBaseDataStore ds,
			final double pct )
			throws IOException {
		final Map<Class<? extends NumericDimensionDefinition>, ConstraintData> constraints = new HashMap<>();
		int dim = 4;
		// while (dim >= 1) {
		// if (x[1] - x[0] > 0) {
		constraints.put(
				XDimension.class,
				new ConstraintData(
						new NumericRange(
								x[0],
								x[1]),
						false));
		// }
		// if (y[1] - y[0] > 0) {
		constraints.put(
				YDimension.class,
				new ConstraintData(
						new NumericRange(
								y[0],
								y[1]),
						false));
		// }
		// if (z[1] - z[0] > 0) {
		constraints.put(
				ZDimension.class,
				new ConstraintData(
						new NumericRange(
								z[0],
								z[1]),
						false));
		// }
		// if (t[1] - t[0] > 0) {
		constraints.put(
				TDimension.class,
				new ConstraintData(
						new NumericRange(
								t[0],
								t[1]),
						false));
		// }
		final double[] time = new double[samples];
		long maxTotal = 0;
		for (int i = 0; i < samples; i++) {

			final StopWatch sw = new StopWatch();
			sw.start();
			try (final CloseableIterator<?> it = ds.query(
					new QueryOptions(
							FourDimensionalDataAdapter.ADAPTER_ID,
							indexId),
					new BasicQuery(
							new Constraints(
									new ConstraintSet(
											constraints))))) {
				long total = 0;
				while (it.hasNext()) {
					total++;
					it.next();
				}
				sw.stop();
				if (i == 0) {
					maxTotal = total;
				}
				else if (maxTotal != total) {
					System.err.println("got " + i + " and " + maxTotal);
					maxTotal = Math.max(
							total,
							maxTotal);
				}

				time[i] = sw.getTime();
			}
		}
		System.err.println(new Statistics(
				time,
				pct,
				maxTotal,
				x[0],
				x[1],
				y[0],
				y[1],
				z[0],
				z[1],
				t[0],
				t[1],
				indexId.getString(),
				dim + "D").toCSVRow());
		if (dim == 4) {
			t[0] = 0;
			t[1] = 0;
		}
		if (dim == 3) {
			z[0] = 0;
			z[1] = 0;
		}
		if (dim == 2) {
			y[0] = 0;
			y[1] = 0;
		}
		dim--;
		// }
	}

	private static void queryIndex(
			final double[] x,
			final double[] y,
			final double[] z,
			final double[] t,
			final ByteArrayId indexId,
			final int samples,
			final HBaseDataStore ds,
			final double pct,
			String queryDimensions )
			throws IOException {
		final Map<Class<? extends NumericDimensionDefinition>, ConstraintData> constraints = new HashMap<>();
		if (x[1] - x[0] > 0) {
			constraints.put(
					XDimension.class,
					new ConstraintData(
							new NumericRange(
									x[0],
									x[1]),
							false));
		}
		if (y[1] - y[0] > 0) {
			constraints.put(
					YDimension.class,
					new ConstraintData(
							new NumericRange(
									y[0],
									y[1]),
							false));
		}
		if (z[1] - z[0] > 0) {
			constraints.put(
					ZDimension.class,
					new ConstraintData(
							new NumericRange(
									z[0],
									z[1]),
							false));
		}
		if (t[1] - t[0] > 0) {
			constraints.put(
					TDimension.class,
					new ConstraintData(
							new NumericRange(
									t[0],
									t[1]),
							false));
		}
		final double[] time = new double[samples];
		long maxTotal = 0;
		for (int i = 0; i < samples; i++) {

			final StopWatch sw = new StopWatch();
			sw.start();
			try (final CloseableIterator<?> it = ds.query(
					new QueryOptions(
							FourDimensionalDataAdapter.ADAPTER_ID,
							indexId),
					new BasicQuery(
							new Constraints(
									new ConstraintSet(
											constraints))))) {
				long total = 0;
				while (it.hasNext()) {
					total++;
					it.next();
				}
				sw.stop();
				if (i == 0) {
					maxTotal = total;
				}
				else if (maxTotal != total) {
					System.err.println("got " + i + " and " + maxTotal);
					maxTotal = Math.max(
							total,
							maxTotal);
				}

				time[i] = sw.getTime();
			}
		}
		System.err.println(new Statistics(
				time,
				pct,
				maxTotal,
				x[0],
				x[1],
				y[0],
				y[1],
				z[0],
				z[1],
				t[0],
				t[1],
				indexId.getString(),
				queryDimensions).toCSVRow());
	}
}
