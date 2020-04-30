package cleansing.processing.events;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import cleansing.processing.base.DataWriter;
import cleansing.processing.base.model.TransformedDataInfo;
import org.apache.spark.sql.SparkSession;

/**
 *	Class that orchestrates writing the events data
 */
@Slf4j
public class EventsWriter extends DataWriter {

	private InvalidEventsWriter invalidEventsWriter = new InvalidEventsWriter();
	private ValidEventsWriter validEventsWriter = new ValidEventsWriter();

	public EventsWriter(SparkSession sparkSession) {
		super(sparkSession);
	}

	/**
	 * Writes all data to appropriate location
	 * @param transformedDataInfo Class containing information about transformed data
	 * @param outputPath Path to the processed data directory in which it will be written after processing
	 * @param errorPath Path to error data directory in which it will be written after processing
	 */
	public void write(@NonNull TransformedDataInfo transformedDataInfo, String outputPath, String errorPath) {

		log.info("Trigger writing invalid events");
		//write invalid events
		invalidEventsWriter.write(transformedDataInfo.getInvalidEvents(), errorPath);

		log.info("Trigger writing valid events");
		//write valid events
		validEventsWriter.write(transformedDataInfo.getValidEvents(), outputPath);

	}
}
