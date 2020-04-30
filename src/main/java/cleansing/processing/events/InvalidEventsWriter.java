package cleansing.processing.events;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

/**
 * Class responsible for writing invalid events
 */
@Slf4j
public class InvalidEventsWriter {
	/**
	 * Writes invalid events to @errorPath location
	 * @param invalidEvents Dataset containing invalid events
	 * @param errorPath Location where invalid events will be written to
	 */
	public void write(@NonNull Dataset<String> invalidEvents, @NonNull String errorPath) {
		//Validate error path parameter
		if (errorPath.isEmpty()) {
			throw new IllegalArgumentException("Error path is empty");
		}

		log.info("Writing invalid events to " + errorPath);
		//Write invalid records in their original form
		invalidEvents
				.write()
				.mode(SaveMode.Overwrite)
				.text(errorPath);
	}
}
