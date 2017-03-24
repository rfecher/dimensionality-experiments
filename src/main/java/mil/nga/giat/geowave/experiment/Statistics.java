package mil.nga.giat.geowave.experiment;

import java.util.Arrays;

public class Statistics
{
	double[] data;
	int size;
	private final double pctPerDimension;
	private final double xMin;
	private final double xMax;
	private final double yMin;
	private final double yMax;
	private final double zMin;
	private final double zMax;
	private final double tMin;
	private final double tMax;
	private String index;
	private String queryDimensions;
	private final long entryCount;

	public Statistics(
			final double[] data,
			final double pctPerDimension,
			final long entryCount,
			double xMin,
			double xMax,
			double yMin,
			double yMax,
			double zMin,
			double zMax,
			double tMin,
			double tMax,
			String index,
			String queryDimensions ) {
		this.data = data;
		this.pctPerDimension = pctPerDimension;
		this.entryCount = entryCount;
		this.xMin = xMin;
		this.xMax = xMax;
		this.yMin = yMin;
		this.yMax = yMax;
		this.zMin = zMin;
		this.zMax = zMax;
		this.tMin = tMin;
		this.tMax = tMax;
		size = data.length;
		this.index = index;
		this.queryDimensions = queryDimensions;
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
		return Math.sqrt(getVariance());
	}

	public double median() {
		Arrays.sort(data);

		if ((data.length % 2) == 0) {
			return (data[(data.length / 2) - 1] + data[data.length / 2]) / 2.0;
		}
		return data[data.length / 2];
	}

	@Override
	public String toString() {
		return "Statistics [data=" + Arrays.toString(data) + ", size=" + size + ", pctPerDimension=" + pctPerDimension
				+ ", entryCount=" + entryCount + "]\n" + toCSVRow();
	}

	public static String getCSVHeader() {
		return "Range Count, Result Count, Mean (ms), Median (ms), Std Dev (ms),xmin,xmax,ymin,ymax,zmin,zmax,tmin,tmax, index";
	}

	public String toCSVRow() {
		return String.format(
				"%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
				pctPerDimension,
				entryCount,
				getMean(),
				median(),
				getStdDev(),
				xMin,
				xMax,
				yMin,
				yMax,
				zMin,
				zMax,
				tMin,
				tMax,
				index,
				queryDimensions);
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
		System.err.println(toCSVRow());
	}
}
