package cleansing.processing.events.model;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * This class validates whether each row contains valid values
 */
@Slf4j
public class RowValidationFunction implements FilterFunction<Row> {

	@Override
	public boolean call(Row row) {
		log.info("Validating data Row");

		boolean isValid = true;
		int eventIdColmnIndex = row.schema().fieldIndex(SchemaConstants.MandatoryDataColumns.EVENT_ID_COLUMN_NAME);
		int eventTypeColmnIndex = row.schema().fieldIndex(SchemaConstants.MandatoryDataColumns.EVENT_TYPE_COLUMN_NAME);
		int eventDateColmnIndex = row.schema().fieldIndex(SchemaConstants.MandatoryDataColumns.EVENT_DATE_COLUMN_NAME);

		String eventDate = row.getString(eventDateColmnIndex);
		String eventId = row.getString(eventIdColmnIndex);
		String eventType = row.getString(eventTypeColmnIndex);

		log.debug(SchemaConstants.MandatoryDataColumns.EVENT_ID_COLUMN_NAME + ": '" + eventId + "'");
		log.debug(SchemaConstants.MandatoryDataColumns.EVENT_TYPE_COLUMN_NAME + ": '" + eventType + "'");
		log.debug(SchemaConstants.MandatoryDataColumns.EVENT_DATE_COLUMN_NAME + ": '" + eventDate + "'");

		try{
			//check that event_date is of correct format
			LocalDateTime.parse(eventDate, DateTimeFormatter.ofPattern(SchemaConstants.MandatoryDataColumns.EVENT_DATE_FORMAT));
			//check that event_id is a valid number
			Long.valueOf(eventId);
			//check that event_type is non empty String
			if(eventType == null || eventType.isEmpty()){
				log.info("Mandatory Row values validation has failed");
				isValid = false;
			}
		}catch (Exception e){
			log.info("Mandatory Row values validation has failed");
			isValid = false;
		}

		log.info("Row validation completed");
		return isValid;
	}
}
