package cleansing.processing.events

import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SaveMode
import org.mockito.Mockito
import org.powermock.api.mockito.PowerMockito
import spock.lang.Specification
import spock.lang.Unroll


class InvalidEventsWriterSpec extends Specification {

    def invalidEventsWriter = new InvalidEventsWriter()

    @Unroll
    def "EventsDataWriter throws exception on NULL parameters: invalidEvents: '#invalid_events_dataset', errorPath: '#error_path'"() {

        when:
        invalidEventsWriter.write(invalid_events_dataset, error_path)

        then:
        thrown(IllegalArgumentException)

        where:
        invalid_events_dataset | error_path || _
        Mock(Dataset)          | null       || _
        null                   | "ePath"    || _
        Mock(Dataset)          | ""         || _
    }

    def "Verify writing flow is correct"() {

        given:
        def mockInvalidDataFrameWriter =  PowerMockito.mock(DataFrameWriter.class)
        def mockInvalidEventsDataset = Mockito.mock(Dataset)
        def errorPath = "ePath"

        when:
        PowerMockito.when(mockInvalidEventsDataset.write()).thenReturn(mockInvalidDataFrameWriter)
        PowerMockito.when(mockInvalidDataFrameWriter.mode(SaveMode.Overwrite)).thenReturn(mockInvalidDataFrameWriter)
        PowerMockito.doNothing().when(mockInvalidDataFrameWriter).text(errorPath)
        invalidEventsWriter.write(mockInvalidEventsDataset, errorPath)

        Mockito.verify(mockInvalidEventsDataset, Mockito.times(1)).write()
        Mockito.verify(mockInvalidDataFrameWriter, Mockito.times(1)).mode(SaveMode.Overwrite)
        Mockito.verify(mockInvalidDataFrameWriter, Mockito.times(1)).text(errorPath)

       then:
       true
    }
}
