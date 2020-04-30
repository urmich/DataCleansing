package cleansing.core;

import org.apache.spark.sql.SparkSession;

/**
 * Convenience class to be extended by other classes that need SparkSession initialized during class initialization
 */
public abstract class SparkAware {

	//property to be used by extending classes
	protected SparkSession sparkSession = SparkEnabler.getInstance().getSparkSession();

}
