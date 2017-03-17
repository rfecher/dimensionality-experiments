package mil.nga.giat.geowave.experiment;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.io.Text;

import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.index.lexicoder.LongLexicoder;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

public class HBaseDimensionalitySensitivity
{
	private static long TOTAL = 100000L;
	private static int SAMPLE_SIZE = 10;

	// there's probably a cap on the ranges before it just takes ridiculously
	// too long (and logically we would never exceed), not sure what it is,
	// probably differs per datastore
	// this is mostly here for sanity purposes so we aren't running queries that
	// may never finish and may be not worth benchmarking because they're just
	// too ridiculous in reality (typically at the finest grained query
	// decomposition we end up with 10's of thousands of ranges, now that could
	// be multiplied by the number of hashes/partitions, but there still is some
	// logical cap on the number of ranges that we'll ever realistically use)
	private static long MAX_RANGES = 1000000L;

	public static void main(
			final String[] args )
			throws Exception {

		HBaseDataStore dataStore = new HBaseDataStore(new BasicHBaseOperations(args[0], args[1]));

		final BatchWriter w = c.createBatchWriter(
				"test",
				new BatchWriterConfig());
		final LongLexicoder lexicoder = Lexicoders.LONG;
		long ctr = 0;
		StopWatch sw = new StopWatch();
		sw.start();
		while (ctr < TOTAL * 2) {
			final Mutation m = new Mutation(
					new Text(
							lexicoder.toByteArray(
									ctr)));
			ctr += 2;
			final byte[] value = new byte[500];
			new Random().nextBytes(
					value);
			m.put(
					new Text(),
					new Text(),
					new Value(
							value));
			w.addMutation(
					m);
		}
		sw.stop();
		w.close();
		c.tableOperations().compact(
				"test",
				null,
				null,
				true,
				true);
		System.err.println(
				"ingest: " + sw.getTime());

		// TODO write a CSV to file
		System.err.println(
				Statistics.getCSVHeader());

		Statistics.printStats(
				allData(
						c,
						1));
		Statistics.printStats(
				allData(
						c,
						2));
		for (long i = 10; i < TOTAL; i *= 10) {
			Statistics.printStats(
					allData(
							c,
							i));
		}
		Statistics.printStats(
				allData(
						c,
						TOTAL / 2));
		Statistics.printStats(
				allData(
						c,
						TOTAL));

		Statistics.printStats(
				oneRange(
						c,
						1));
		Statistics.printStats(
				oneRange(
						c,
						2));
		for (long i = 10; i < TOTAL; i *= 10) {
			Statistics.printStats(
					oneRange(
							c,
							i));
		}
		Statistics.printStats(
				oneRange(
						c,
						TOTAL / 2));
		Statistics.printStats(
				skipIntervals(
						c,
						1,
						2));
		Statistics.printStats(
				skipIntervals(
						c,
						2,
						4));
		for (long i = 10; (i * 10) < TOTAL; i *= 10) {
			Statistics.printStats(
					skipIntervals(
							c,
							i,
							i * 10));
		}
		a.tearDown();
		z.tearDown();
	}

	private static Statistics allData(
			final Connector c,
			final long interval )
			throws TableNotFoundException {
		final LongLexicoder lexicoder = Lexicoders.LONG;
		double[] scanResults = new double[SAMPLE_SIZE];
		long rangeCnt = 0;
		long expectedResults = TOTAL;
		if (TOTAL / interval > MAX_RANGES) {
			return null;
		}
		for (int i = 0; i < SAMPLE_SIZE; i++) {
			final StopWatch sw = new StopWatch();
			final BatchScanner s = c.createBatchScanner(
					"test",
					new Authorizations(),
					16);
			List<Range> ranges = new ArrayList<Range>();
			for (long j = 0; j < TOTAL * 2; j += (interval * 2)) {
				ranges.add(
						new Range(
								new Text(
										lexicoder.toByteArray(
												j)),
								true,

								new Text(
										lexicoder.toByteArray(
												j + interval * 2 - 1)),
								false));
			}
			s.setRanges(
					ranges);
			rangeCnt = ranges.size();
			final Iterator<Entry<Key, Value>> it = s.iterator();
			long ctr = 0;
			sw.start();
			while (it.hasNext()) {
				it.next();
				ctr++;
			}
			sw.stop();

			if (ctr != TOTAL) {
				System.err.println(
						"experimentFullScan " + interval + " " + ctr);
			}
			scanResults[i] = sw.getTime();
			s.close();
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	}

	private static Statistics skipIntervals(
			final Connector c,
			final long interval,
			final long skipCnt )
			throws TableNotFoundException {
		final LongLexicoder lexicoder = Lexicoders.LONG;
		double[] scanResults = new double[SAMPLE_SIZE];
		long rangeCnt = 0;
		long expectedResults = (long) Math.ceil(
				(double) TOTAL / (double) skipCnt) * interval;
		for (int i = 0; i < SAMPLE_SIZE; i++) {
			final StopWatch sw = new StopWatch();
			final BatchScanner s = c.createBatchScanner(
					"test",
					new Authorizations(),
					16);
			List<Range> ranges = new ArrayList<Range>();
			for (long j = 0; j < TOTAL * 2; j += (skipCnt * 2)) {
				ranges.add(
						new Range(
								new Text(
										lexicoder.toByteArray(
												j)),
								true,

								new Text(
										lexicoder.toByteArray(
												j + (interval * 2))),
								false));
			}
			if (ranges.size() > MAX_RANGES) {
				return null;
			}
			s.setRanges(
					ranges);
			rangeCnt = ranges.size();
			final Iterator<Entry<Key, Value>> it = s.iterator();
			long ctr = 0;
			sw.start();
			while (it.hasNext()) {
				it.next();
				ctr++;
			}
			sw.stop();

			if (ctr != expectedResults) {
				System.err.println(
						"experimentSkipScan " + interval + " " + ctr);
			}
			scanResults[i] = sw.getTime();
			s.close();
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	}

	private static Statistics oneRange(
			final Connector c,
			final long cnt )
			throws TableNotFoundException {
		final LongLexicoder lexicoder = Lexicoders.LONG;
		double[] scanResults = new double[SAMPLE_SIZE];
		long rangeCnt = 0;
		long expectedResults = cnt;
		for (int i = 0; i < SAMPLE_SIZE; i++) {
			final StopWatch sw = new StopWatch();
			final BatchScanner s = c.createBatchScanner(
					"test",
					new Authorizations(),
					16);
			List<Range> ranges = new ArrayList<Range>();
			long start = (TOTAL * 2 - cnt * 2) / 2L;
			ranges.add(
					new Range(
							new Text(
									lexicoder.toByteArray(
											start)),
							true,

							new Text(
									lexicoder.toByteArray(
											start + cnt * 2)),
							false));
			s.setRanges(
					ranges);
			rangeCnt = ranges.size();
			final Iterator<Entry<Key, Value>> it = s.iterator();
			long ctr = 0;
			sw.start();
			while (it.hasNext()) {
				it.next();
				ctr++;
			}
			sw.stop();

			if (ctr != cnt) {
				System.err.println(
						"extraData " + cnt + " " + ctr);
			}
			scanResults[i] = sw.getTime();
			s.close();
		}
		return new Statistics(
				scanResults,
				rangeCnt,
				expectedResults);
	}
}
