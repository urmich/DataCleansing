package cleansing.processing.events;

import cleansing.processing.base.DataProcessor;
import cleansing.processing.base.model.TransformedDataInfo;
import org.apache.spark.sql.Dataset;

/**
 * Class to process events data
 */
public class EventsProcessor extends DataProcessor<String> {

	private EventsReader eventsReader = new EventsReader(sparkSession);
	private EventsTransformer eventsTransformer = new EventsTransformer(sparkSession);
	private EventsWriter eventsWriter = new EventsWriter(sparkSession);

	/**
	 * Reads events from @inputPath location
	 * @param inputPath Path to raw data files/directory
	 * @return Dataset<String> with read events
	 */
	public Dataset<String> read(String inputPath) {
		return eventsReader.read(inputPath);
	}

	/**
	 * Transforms data
	 * @param data DataFrame with data to be transformed
	 * @return TransformedDataInfo container with transformed data
	 */
	public TransformedDataInfo transform(Dataset<String> data) {
		return eventsTransformer.transform(data);
	}

	/**
	 * Writes valid events to @outputPath and error events to @errorPath
	 * @param transformedDataInfo Container of transformed data
	 * @param outputPath Path to the processed data directory in which it will be written after processing
	 * @param errorPath Path to error data directory in which it will be written after processing
	 */
	public void write(TransformedDataInfo transformedDataInfo, String outputPath, String errorPath) {
		eventsWriter.write(transformedDataInfo, outputPath, errorPath);
	}
}
