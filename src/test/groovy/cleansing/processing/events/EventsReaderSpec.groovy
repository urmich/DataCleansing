package cleansing.processing.events

import cleansing.processing.events.model.SchemaConstants
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import spock.lang.Specification

class EventsReaderSpec extends Specification {

    def mockSparkSession = Mock(SparkSession)
    def eventsReader = new EventsReader(mockSparkSession)

    def "read data throws exception on NULL location"() {
        when:
        eventsReader.read(null)

        then:
        thrown(IllegalArgumentException)
        and:
        0 * mockSparkSession._(*_)
    }

    def "Test valid reading of events"() {
        given:
        StructField[] structFields = new StructField[1]
        structFields[0] = new StructField(SchemaConstants.RAW_EVENTS_COLUMN_NAME, DataTypes.StringType, true, Metadata.empty())
        def dfSchema = new StructType(structFields)
        def mockDataFrameReader = Mock(DataFrameReader)
        def mockDataset = Mock(Dataset)
        def dataLocation = "/myLocation"

        when:
        def data = eventsReader.read(dataLocation)

        then:
        data != null
        and:
        1 * mockSparkSession.read() >> mockDataFrameReader
        with(mockDataFrameReader) {
            1 * schema(dfSchema) >> mockDataFrameReader
            1 * text(dataLocation) >> mockDataset
        }
        1 * mockDataset.as((Encoder) _) >> mockDataset
    }
}
