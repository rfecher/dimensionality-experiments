package mil.nga.giat.geowave.experiment;

import java.util.Arrays;

public class Statistics
{
	double[] data;
	int size;
	private final long rangeCount;
	private final long entryCount;

	public Statistics(
			final double[] data,
			final long rangeCount,
			final long entryCount ) {
		this.data = data;
		this.rangeCount = rangeCount;
		this.entryCount = entryCount;
		size = data.length;
	}

	double getMean() {
		double sum = 0.0;
		for (final double a : data) {
			sum += a;
		}
		return sum / size;
	}

	double getVariance() {
		final double mean = getMean();
		double temp = 0;
		for (final double a : data) {
			temp += (a - mean) * (a - mean);
		}
		return temp / size;
	}

	double getStdDev() {
		return Math.sqrt(
				getVariance());
	}

	public double median() {
		Arrays.sort(
				data);

		if ((data.length % 2) == 0) {
			return (data[(data.length / 2) - 1] + data[data.length / 2]) / 2.0;
		}
		return data[data.length / 2];
	}

	@Override
	public String toString() {
		return "Statistics [data=" + Arrays.toString(
				data) + ", size=" + size + ", rangeCount=" + rangeCount + ", entryCount=" + entryCount + "]\n"
				+ toCSVRow();
	}

	public static String getCSVHeader() {
		return "Range Count, Result Count, Mean (ms), Median (ms), Std Dev (ms)";
	}

	public String toCSVRow() {
		return String.format(
				"%s,%s,%s,%s,%s",
				rangeCount,
				entryCount,
				getMean(),
				median(),
				getStdDev());
	}

	public static void printStats(
			final Statistics stats ) {
		// TODO write it to a file instead
		if (stats != null) {
			stats.printStats();
		}

	}

	public void printStats() {
		// TODO write it to a file instead
		System.err.println(
				toCSVRow());
	}
}
