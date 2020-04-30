package cleansing.processing.events

import cleansing.processing.base.model.TransformedDataInfo
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import spock.lang.Specification
import spock.lang.Unroll

class EventsWriterSpec extends Specification {

    def mockSparkSession = Mock(SparkSession)
    def mockInvalidEventsWriter = Mock(InvalidEventsWriter)
    def mockValidEventsWriter = Mock(ValidEventsWriter)
    def eventsWriter = new EventsWriter(mockSparkSession)

    @Unroll
    def "EventsDataWriter throws exception on NULL parameters"() {

        when:
        eventsWriter.write(null, "/output", "/error")

        then:
        thrown(IllegalArgumentException)
    }

    def "Verify writing flow is correct"() {
        given:
        def mockDataset = Mock(Dataset)
        def mockTransformedDataInfo = Mock(TransformedDataInfo)

        def outputPath = "oPath"
        def errorPath = "ePath"

        eventsWriter.validEventsWriter = mockValidEventsWriter
        eventsWriter.invalidEventsWriter = mockInvalidEventsWriter

        when:
        eventsWriter.write(mockTransformedDataInfo, outputPath, errorPath)

        then:
        1 * mockTransformedDataInfo.getInvalidEvents() >> mockDataset
        1 * mockInvalidEventsWriter.write(mockDataset, errorPath)
        1 * mockTransformedDataInfo.getValidEvents() >> mockDataset
        1 * mockValidEventsWriter.write(mockDataset, outputPath)
    }
}
