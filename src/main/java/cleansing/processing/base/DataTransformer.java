package cleansing.processing.base;

import cleansing.core.SparkActor;
import cleansing.processing.base.model.TransformedDataInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Base abstract class for data transformer
 * @param <R> Datatype of data to be transformed
 */
public abstract class DataTransformer<R> extends SparkActor {

	public DataTransformer(SparkSession sparkSession) {
		super(sparkSession);
	}

	/**
	 * @param data Dataset of type <italic>R</italic> - data to be transformed
	 * @return Transformed data
	 */
	public abstract TransformedDataInfo transform(Dataset<R> data);
}
