package cleansing.processing.events;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import cleansing.config.AppConfig;
import cleansing.processing.events.model.SchemaConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

/**
 * Class responsible for writing valid events
 */
@Slf4j
public class ValidEventsWriter {

	/**
	 * Writes valid events to @outputPath location
	 *
	 * @param validEvents Dataset containing valid events
	 * @param outputPath  Location where valid events will be written to
	 */

	public void write(@NonNull Dataset<Row> validEvents, @NonNull String outputPath) {

		//Validate output path parameter
		if (outputPath.isEmpty()) {
			throw new IllegalArgumentException("Output path is empty");
		}

		log.info("Preparing the valid events DF for partitioning, bucketing and sorting by adding additional columns to DF");
		//Prepare data for writing in desired format and according to requirements (partitions, bucketing, ordering)
		Dataset<Row> validRecordsWithPartitionColumns = validEvents
				.withColumn(SchemaConstants.DAY_PARTITION_COLUMN_NAME,
						substring(col(SchemaConstants.MandatoryDataColumns.EVENT_DATE_COLUMN_NAME),
								0,
								SchemaConstants.MandatoryDataColumns.EVENT_DATE_FORMAT.split(" ")[0].length()))
				.withColumn(SchemaConstants.DAY_PARTITION_COLUMN_NAME,
						regexp_replace(col(SchemaConstants.DAY_PARTITION_COLUMN_NAME), "\\-", ""))
				.withColumn(SchemaConstants.EVENT_TYPE_PARTITION_COLUMN_NAME,
						col(SchemaConstants.MandatoryDataColumns.EVENT_TYPE_COLUMN_NAME))
				.withColumn(SchemaConstants.MandatoryDataColumns.EVENT_ID_COLUMN_NAME,
						col(SchemaConstants.MandatoryDataColumns.EVENT_ID_COLUMN_NAME).cast(DataTypes.LongType))
				.drop(SchemaConstants.RAW_EVENTS_COLUMN_NAME);

		log.info("Writing valid events to " + outputPath);
		validRecordsWithPartitionColumns
				.write()
				.option("path", outputPath)
				.partitionBy(SchemaConstants.DAY_PARTITION_COLUMN_NAME, SchemaConstants.EVENT_TYPE_PARTITION_COLUMN_NAME)
				.bucketBy(AppConfig.DATA_BUCKETS, SchemaConstants.MandatoryDataColumns.EVENT_ID_COLUMN_NAME)
				.sortBy(SchemaConstants.MandatoryDataColumns.EVENT_ID_COLUMN_NAME)
				.mode(SaveMode.Overwrite)
				.saveAsTable("valid_events");
	}
}
