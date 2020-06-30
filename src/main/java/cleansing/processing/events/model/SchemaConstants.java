package cleansing.processing.events.model;

/**
 * Class that holds constants related to data schema
 */
public class SchemaConstants {

	public class MandatoryDataColumns {
		public static final String EVENT_ID_COLUMN_NAME = "event_id";
		public static final String EVENT_TYPE_COLUMN_NAME = "event_type";
		public static final String EVENT_DATE_COLUMN_NAME = "event_date";
		public static final String EVENT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	}

	public class ValidatedEventColumns{
		public static final String RAW_EVENT_COLUMN_NAME = "rawEvent";
		public static final String VALIDITY_INDICATOR_EVENT_COLUMN = "rawEvent";
	}

	public static final String RAW_EVENTS_COLUMN_NAME = "raw_event";

	public static final String DAY_PARTITION_COLUMN_NAME = "day";

	public static final String EVENT_TYPE_PARTITION_COLUMN_NAME = "eventType";

	public static final int VALID_EVENT = 1;
	public static final int INVALID_EVENT = 0;

}
