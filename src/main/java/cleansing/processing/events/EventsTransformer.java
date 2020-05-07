package cleansing.processing.events;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import cleansing.processing.base.DataTransformer;
import cleansing.processing.base.model.TransformedDataInfo;
import cleansing.processing.events.model.RowValidationFunction;
import cleansing.processing.events.model.SchemaConstants;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

/**
 * Events transformer class
 */
@Slf4j
public class EventsTransformer extends DataTransformer<String> {

	public EventsTransformer(SparkSession sparkSession) {
		super(sparkSession);
	}

	/**
	 * Method that execute actual data transformation from raw events to valid and invalid events
	 *
	 * @param data Dataset of type <italic>R</italic> - data to be transformed
	 * @return
	 */
	public TransformedDataInfo transform(@NonNull Dataset<String> data) {

		log.info("Starting data transformation");

		// Add sequential ID to the DF for join
		Dataset<Row> dfWithId = data.withColumn(SchemaConstants.JOIN_ID_COLUMN_NAME, monotonically_increasing_id());
		log.info("ID column has been added to raw data");

		log.info("Convert raw JSONs to DF of JSON values");
		// Parse raw data to JSON
		//Put unparsable records to dedicated column
		// Add sequential ID to the DF for join
		Dataset<Row> explodedDF = sparkSession
				.read()
				.option("columnNameOfCorruptRecord", SchemaConstants.BAD_RECORD_COLUMN_NAME)
				.json(data)
				.withColumn(SchemaConstants.JOIN_ID_COLUMN_NAME, monotonically_increasing_id());

		log.info("JSONs conversion completed");

		log.info("Building exploded DF schema");
		//check if "data" column exist, i.e. "data" block was not empty at least once
		boolean dataFieldIsPresent = false;
		String[] explodedDFColumns = explodedDF.columns();
		for (int i = 0; i < explodedDF.columns().length; i++) {
			if (SchemaConstants.OptionalColumns.DATA_COLUMN_NAME.equals(explodedDFColumns[i])) {
				dataFieldIsPresent = true;
				break;
			}
		}

		Column[] wholeDFColumns;
		if (dataFieldIsPresent) {
			wholeDFColumns = new Column[]{col(SchemaConstants.MandatoryDataColumns.EVENT_ID_COLUMN_NAME),
					col(SchemaConstants.MandatoryDataColumns.EVENT_TYPE_COLUMN_NAME),
					col(SchemaConstants.MandatoryDataColumns.EVENT_DATE_COLUMN_NAME),
					col(SchemaConstants.BAD_RECORD_COLUMN_NAME),
					col(SchemaConstants.OptionalColumns.ALL_DATA_COLUMNS),
					col(SchemaConstants.RAW_EVENTS_COLUMN_NAME)};
		} else {
			wholeDFColumns = new Column[]{col(SchemaConstants.MandatoryDataColumns.EVENT_ID_COLUMN_NAME),
					col(SchemaConstants.MandatoryDataColumns.EVENT_TYPE_COLUMN_NAME),
					col(SchemaConstants.MandatoryDataColumns.EVENT_DATE_COLUMN_NAME),
					col(SchemaConstants.BAD_RECORD_COLUMN_NAME),
					col(SchemaConstants.RAW_EVENTS_COLUMN_NAME)};
		}

		log.info("Exploded DF schema contains " + wholeDFColumns.length + " columns");

		log.info("Combining raw data with exploded JSONs values");
		//Join both DFs to get original records in the DF together with parsed JSONs
		Dataset<Row> wholeDF = explodedDF
				.join(dfWithId, SchemaConstants.JOIN_ID_COLUMN_NAME)
				.drop(SchemaConstants.JOIN_ID_COLUMN_NAME)
				.select(wholeDFColumns);

		log.info("Extracting unparsable events into separate DF");
		//Obtain only unparsable records
		Dataset<Row> badRecordsDF = wholeDF.
				select(SchemaConstants.BAD_RECORD_COLUMN_NAME).
				filter(SchemaConstants.BAD_RECORD_COLUMN_NAME + " is not null");

		log.info("Extracting parsed events into separate DF");
		//Create DF with only parsed JSONs
		Dataset<Row> parsedRecordsDF = wholeDF.
				filter(SchemaConstants.BAD_RECORD_COLUMN_NAME + " is null")
				.drop(SchemaConstants.BAD_RECORD_COLUMN_NAME);

		//release memory
		wholeDF.unpersist();
		log.info("Releasing memory of combined DF");
		
		log.info("Build DF with only valid events");
		//Create DF with only valid parsed records (that contain only good values)
		//And remove unnecessary column
		Dataset<Row> validRecordsDF = parsedRecordsDF.filter(new RowValidationFunction());

		log.info("Build DF with only invalid events");
		//Create DF that contains invalid parsed records (that contain only bad values)
		Dataset<Row> invalidRecordsDF = parsedRecordsDF.except(validRecordsDF);

		//Release memory
		parsedRecordsDF.unpersist();
		log.info("Releasing memory of DF of parsed events");

		log.info("Create DF with only column of raw events and rename the column for UNION");
		//Create DF with only column of raw events and rename the column for UNION
		Dataset<Row> badOrigRecordsDF = invalidRecordsDF
				.select(col(SchemaConstants.RAW_EVENTS_COLUMN_NAME)
						.as(SchemaConstants.BAD_RECORD_COLUMN_NAME));

		log.info("Create DF with ALL bad records by adding invalidRecords to bad records");
		//Create DF with ALL bad records by adding invalidRecords to bad records
		Dataset<String> allBadRecordsDF = badRecordsDF.union(badOrigRecordsDF).as(Encoders.STRING());

		TransformedDataInfo transformedDataInfo = new TransformedDataInfo(validRecordsDF, allBadRecordsDF);

		log.info("Returning valid and invalid events DFs");
		return transformedDataInfo;
	}
}