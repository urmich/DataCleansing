package cleansing.processing.events

import cleansing.core.LocalSparkTestUtil
import cleansing.processing.base.model.TransformedDataInfo
import spock.lang.Specification
import spock.lang.Unroll


class EventsTransformerIntegrationSpec extends Specification {

    @Unroll
    def "Verify number of valid and invalid rows in file '#data_file' - valid: '#valid_count', invalid: '#invalid_count'"() {

        given:

        def sparkSession = LocalSparkTestUtil.getSparkSession()
        def eventsReader = new EventsReader(sparkSession)
        def inputDataset = eventsReader.read(data_file)
        def eventsTransformer = new EventsTransformer(sparkSession)

        when:
        TransformedDataInfo<String> transformedDataInfo = eventsTransformer.transform(inputDataset)

        then:
        transformedDataInfo.getValidEvents().count() == valid_count
        transformedDataInfo.getInvalidEvents().count() == invalid_count

        where:
        data_file                                                                       | valid_count | invalid_count || _
        "src/test/resources/data/transformer_test/transformer_test_no_data_block.txt"   | 1           | 5             || _
        "src/test/resources/data/transformer_test/transformer_test_with_data_block.txt" | 2           | 5             || _
    }
}
