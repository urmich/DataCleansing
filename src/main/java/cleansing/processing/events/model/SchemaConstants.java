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

	public static final String RAW_EVENTS_COLUMN_NAME = "raw_event";

	public static final String BAD_RECORD_COLUMN_NAME = "bad_record";

	public static final String JOIN_ID_COLUMN_NAME = "id";

	public class OptionalColumns {
		public static final String ALL_DATA_COLUMNS = "data.*";
		public static final String DATA_COLUMN_NAME = "data";
	}

	public static final String DAY_PARTITION_COLUMN_NAME = "day";

	public static final String EVENT_TYPE_PARTITION_COLUMN_NAME = "eventType";

}
