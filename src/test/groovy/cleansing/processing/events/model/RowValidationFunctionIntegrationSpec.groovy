package cleansing.processing.events.model

import cleansing.core.LocalSparkTestUtil
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.DataTypes
import spock.lang.Specification
import spock.lang.Unroll
import static org.apache.spark.sql.functions.*

class RowValidationFunctionIntegrationSpec extends Specification {

    @Unroll
    def "Filtering out invalid records from DF containing all parsed JSONs with event_id: '#event_id', event_type: '#event_type', event_date: '#event_date'"() {

        given:
        //instantiate Spark
        def sparkSession = LocalSparkTestUtil.getSparkSession()

        //prepare data
        def sampleRowString = "{\"event_id\":\"" + event_id + "\",\"event_type\":\"" + event_type + "\",\"event_date\":\"" + event_date + "\"}"
        def aaa = "{\"event_id\":222,\"event_type\":\"et1\",\"event_date\":\"2020-02-20 20:20:20\"}"

        def inputDataset = (Dataset<String>) sparkSession
                .createDataset([sampleRowString], Encoders.STRING())

        def jsonRow = sparkSession
                .read()
                .json(inputDataset)
                .select(col(SchemaConstants.MandatoryDataColumns.EVENT_ID_COLUMN_NAME).cast(DataTypes.StringType),
                        col(SchemaConstants.MandatoryDataColumns.EVENT_TYPE_COLUMN_NAME).cast(DataTypes.StringType),
                        col(SchemaConstants.MandatoryDataColumns.EVENT_DATE_COLUMN_NAME).cast(DataTypes.StringType))
                .collect()[0]

        def filerFunction = new RowValidationFunction()

        when:
        def isValid = filerFunction.call(jsonRow)

        then:
        isValid == result

        where:
        event_id | event_type | event_date                || result
        111      | "aa"       | "2020-02-20 20:20:20"     || true
        "aaa"    | "aa"       | "2020-02-20 20:20:20"     || false
        null     | "aa"       | "2020-02-20 20:20:20"     || false
        111      | ""         | "2020-02-20 20:20:20"     || false
        111      | "aa"       | "2020-02-20 20:20:20.562" || false
        111      | "aa"       | "2020-02-20 20:20:"       || false
        111      | "aa"       | "2020-02-20"              || false
        111      | "aa"       | ""                        || false
        111      | "aa"       | null                      || false
    }

    @Unroll
    def "Filtering out invalid records from DF with parts missing in JSON"() {

        given:
        //instantiate Spark
        def sparkSession = LocalSparkTestUtil.getSparkSession()

        //prepare data
        def goodRow = "{\"event_id\":222,\"event_type\":\"et1\",\"event_date\":\"2020-02-20 20:20:20\"}"
        def badRow1 = "{\"event_type\":\"et1\",\"event_date\":\"2020-02-20 20:20:20\"}"
        def badRow2 = "{\"event_id\":222,\"event_date\":\"2020-02-20 20:20:20\"}"
        def badRow3 = "{\"event_id\":222,\"event_type\":\"et1\"}"
        def inputDataset = (Dataset<String>) sparkSession
                .createDataset([goodRow, badRow1, badRow2, badRow3], Encoders.STRING())

        def jsonDF = sparkSession
                .read()
                .json(inputDataset)
                .select(col(SchemaConstants.MandatoryDataColumns.EVENT_ID_COLUMN_NAME).cast(DataTypes.StringType),
                        col(SchemaConstants.MandatoryDataColumns.EVENT_TYPE_COLUMN_NAME).cast(DataTypes.StringType),
                        col(SchemaConstants.MandatoryDataColumns.EVENT_DATE_COLUMN_NAME).cast(DataTypes.StringType))


        def count = jsonDF.count()
        def filerFunction = new RowValidationFunction()

        when:
        List isValid = new ArrayList<Boolean>()

        for (int i = 0; i < count; i++) {
            isValid.add(filerFunction.call(jsonDF.take(i + 1).last()))
        }

        then:
        isValid.get(rowIndex) == result

        where:
        rowIndex || result
        0        || true
        1        || false
        2        || false
        3        || false
    }
}