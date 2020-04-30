package cleansing.processing.base;

import cleansing.core.SparkActor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Base abstract class for data reader
 * @param <R> Data type of data returned by @read method
 */
public abstract class DataReader<R> extends SparkActor {

	public DataReader(SparkSession sparkSession){
		super(sparkSession);
	}
	/**
	 * Abstract method for reading data
	 * @param dataLocation Path to data files location on FS
	 * @return DataFrame with read data
	 */
	public abstract Dataset<R> read(String dataLocation);
}
