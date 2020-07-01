package cleansing.processing.events;

import cleansing.processing.events.model.RawEventValidityIndicator;
import cleansing.processing.events.model.RawEventsToValidityIndicatorMapPartitionFunction;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import cleansing.processing.base.DataTransformer;
import cleansing.processing.base.model.TransformedDataInfo;
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
     * @return TransformedDataInfo
     */
    public TransformedDataInfo transform(@NonNull Dataset<String> data) {

        log.info("Starting data transformation");

        Dataset<RawEventValidityIndicator> validatedEventsDF =
                data.mapPartitions(new RawEventsToValidityIndicatorMapPartitionFunction(),
                        Encoders.bean(RawEventValidityIndicator.class));

        log.info("Build DF with only valid events");
        //Create DF with only valid parsed records (that contain only good values)
        Dataset<String> validRecordsDF = validatedEventsDF
                .filter(SchemaConstants.ValidatedEventColumns.VALIDITY_INDICATOR_EVENT_COLUMN + " == " + SchemaConstants.VALID_EVENT)
                .select(col(SchemaConstants.ValidatedEventColumns.RAW_EVENT_COLUMN_NAME)
                        .alias(SchemaConstants.RAW_EVENTS_COLUMN_NAME))
                .as(Encoders.STRING());

        log.info("Build DF with only invalid events");
        //Create DF that contains invalid parsed records (that contain only bad values)
        Dataset<String> invalidRecordsDF = validatedEventsDF
                .filter(SchemaConstants.ValidatedEventColumns.VALIDITY_INDICATOR_EVENT_COLUMN + " == " + SchemaConstants.INVALID_EVENT)
                .select(col(SchemaConstants.ValidatedEventColumns.RAW_EVENT_COLUMN_NAME)
                        .alias(SchemaConstants.RAW_EVENTS_COLUMN_NAME))
                .as(Encoders.STRING());

        TransformedDataInfo transformedDataInfo = new TransformedDataInfo(validRecordsDF, invalidRecordsDF);

        log.info("Returning valid and invalid events DFs");
        return transformedDataInfo;
    }
}