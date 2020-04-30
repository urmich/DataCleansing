package cleansing.processing.events

import org.apache.spark.sql.Dataset
import spock.lang.Specification
import spock.lang.Unroll

class ValidEventsWriterSpec extends Specification {

    def validEventsWriter = new ValidEventsWriter()

    @Unroll
    def "EventsDataWriter throws exception on NULL parameters: validEvents: '#valid_events_dataset', outputPath: '#output_path'"() {

        when:
        validEventsWriter.write(valid_events_dataset, output_path)

        then:
        thrown(IllegalArgumentException)

        where:
        valid_events_dataset | output_path || _
        Mock(Dataset)        | null        || _
        null                 | "oPath"     || _
        Mock(Dataset)        | ""          || _
    }
}
