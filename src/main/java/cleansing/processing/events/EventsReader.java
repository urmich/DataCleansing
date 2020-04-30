package cleansing.processing.events;

import lombok.extern.slf4j.Slf4j;
import cleansing.processing.events.model.SchemaConstants;
import cleansing.processing.base.DataReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Events reader class
 */
@Slf4j
public class EventsReader extends DataReader<String> {

	public EventsReader(SparkSession sparkSession) {
		super(sparkSession);
	}

	@Override
	/**
	 * Method that implemnts reading events from the FS
	 * @param dataLocation Path to data files location on FS
	 * @return Dataset with raw events in String format
	 */
	public Dataset<String> read(String dataLocation) {

		if (dataLocation == null || dataLocation.isEmpty()) {
			log.error("Empty dataLocation parameter passed to EventsReader.read");
			throw new IllegalArgumentException("Invalid input parameter dataLocation = '" + dataLocation + "'");
		}

		log.info("Starting reading the data from " + dataLocation);

		Dataset<String> rawEvents;

		StructField[] structFields = new StructField[]{
				new StructField(SchemaConstants.RAW_EVENTS_COLUMN_NAME, DataTypes.StringType, true, Metadata.empty())
		};

		StructType schema = new StructType(structFields);

		rawEvents = sparkSession
				.read()
				.schema(schema)
				.text(dataLocation)
				.as(Encoders.STRING());

		log.info("Data has been read from " + dataLocation);

		return rawEvents;
	}
}
