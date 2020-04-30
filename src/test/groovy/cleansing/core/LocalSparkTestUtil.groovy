package cleansing.core

import org.apache.spark.sql.SparkSession

class LocalSparkTestUtil {
    static SparkSession getSparkSession(){
        return SparkSession
        .builder()
        .master("local[*]")
        .getOrCreate()
    }
}
