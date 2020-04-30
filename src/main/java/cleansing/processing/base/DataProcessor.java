package cleansing.processing.base;

import lombok.extern.slf4j.Slf4j;
import cleansing.core.SparkAware;
import cleansing.processing.base.model.TransformedDataInfo;
import org.apache.spark.sql.Dataset;

/**
 * Abstract class that defines the template of data processing
 * @param <R> The data type of data that is returned by @read() method
 */
@Slf4j
public abstract class DataProcessor<R> extends SparkAware {

	/**
	 * Abstract method for orchestration of reading data
	 * @param inputPath Path to raw data files/directory
	 * @return Dataset of T types of read data
	 */
	public abstract Dataset<R> read(String inputPath);

	/**
	 * Abstract method for orchestration of transforming data
	 * @param data DataFrame with data to be transformed
	 * @return TransformedDataInfo - Container with transformed data
	 */
	public abstract TransformedDataInfo transform(Dataset<R> data);

	/**
	 * Abstract method to write transformed data
	 * @param transformedDataInfo Container of transformed data
	 * @param outputPath Path to the processed data directory in which it will be written after processing
	 * @param errorPath Path to error data directory in which it will be written after processing
	 */

	public abstract void write (TransformedDataInfo transformedDataInfo, String outputPath, String errorPath);

	/**
	 * Template method that implements the flow of data from the source to target.
	 * This method cannot be overriden to prevent changing the template
	 * @param inputPath Path to raw data files/directory
	 * @param outputPath Path to the processed data directory in which it will be written after processing
	 * @param errorPath Path to error data directory in which it will be written after processing
	 */
	public final void process(String inputPath, String outputPath, String errorPath){

		log.info("Starting data processing template");

		Dataset<R> readData = read(inputPath);

		log.info("Data reading complete");

		TransformedDataInfo transformedDataInfo = transform(readData);

		log.info("Data transformation complete");

		write(transformedDataInfo, outputPath, errorPath);

		log.info("Data writing complete");

		log.info("Stopping Spark");
		sparkSession.stop();
	}
}
