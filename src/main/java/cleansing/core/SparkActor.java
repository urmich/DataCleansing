package cleansing.core;

import org.apache.spark.sql.SparkSession;

/**
 * Base class for classes that hold SparkSession and process data with Spark
 */
public abstract class SparkActor {

	protected SparkSession sparkSession;

	public SparkActor(SparkSession sparkSession){
		this.sparkSession = sparkSession;
	}
}
