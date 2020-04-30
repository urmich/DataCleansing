package cleansing.processing.base;

import cleansing.core.SparkActor;
import cleansing.processing.base.model.TransformedDataInfo;
import org.apache.spark.sql.SparkSession;

/**
 * Base class for Data writer
 */
public abstract class DataWriter extends SparkActor {

	public DataWriter(SparkSession sparkSession) {
		super(sparkSession);
	}

	/**
	 * Abstract method for writing data
	 * @param transformedDataInfo Class containing information about transformed data
	 * @param outputPath Path to the processed data directory in which it will be written after processing
	 * @param errorPath Path to error data directory in which it will be written after processing
	 */
	public abstract void write(TransformedDataInfo transformedDataInfo, String outputPath, String errorPath);
}
