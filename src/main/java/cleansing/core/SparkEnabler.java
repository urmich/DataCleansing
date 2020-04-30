package cleansing.core;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * Singleton that created instance of SparkSession
 */
public class SparkEnabler {

	private static SparkEnabler instance;
	private SparkSession sparkSession;

	private SparkEnabler() {
		SparkConf sparkConf = new SparkConf();

		sparkSession = SparkSession
				.builder()
				.config(sparkConf)
				.enableHiveSupport()
				.getOrCreate();
	}

	public static SparkEnabler getInstance() {

		if (instance == null) {
			synchronized (SparkEnabler.class) {
				if (instance == null) {
					instance = new SparkEnabler();
				}
			}
		}
		return instance;
	}

	/**
	 * Method to get SparkSession
	 *
	 * @return SparkSession
	 */
	public SparkSession getSparkSession() {
		return sparkSession;
	}
}
