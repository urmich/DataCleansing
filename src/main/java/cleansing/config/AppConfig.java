package cleansing.config;

/**
 * Class responsible for retrieving and storing external application configuration
 */
public class AppConfig {

	//Number of buckets is a function of quantity of data in each partition, desired file size, etc.
	public final static int DATA_BUCKETS = 10;
}
