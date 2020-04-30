package cleansing.processing.base.model;

import lombok.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Class to be used to contain transformed info
 */
@AllArgsConstructor
public class TransformedDataInfo<R> {

	/**
	 * Transformed valid events
	 */
	@Getter
	private Dataset<Row> validEvents;

	/**
	 * Invalid events in the form that thy were read by the Reader
	 */
	@Getter
	private Dataset<R> invalidEvents;
}
