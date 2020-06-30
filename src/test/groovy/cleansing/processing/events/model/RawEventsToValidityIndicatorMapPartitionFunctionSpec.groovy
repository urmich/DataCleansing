package cleansing.processing.events.model

import spock.lang.Specification
import spock.lang.Unroll

class RawEventsToValidityIndicatorMapPartitionFunctionSpec extends Specification {

    @Unroll
    def "Mapping records containing all parsed JSONs with event_id: '#event_id', event_type: '#event_type', event_date: '#event_date'"() {

        given:
        //prepare data
        def sampleRowString = "{\"event_id\":\"" + event_id + "\",\"event_type\":\"" + event_type + "\",\"event_date\":\"" + event_date + "\"}"

        def list = new ArrayList<String>()
        list.add(sampleRowString)
        def iterator = list.iterator()

        def mappingFunction = new RawEventsToValidityIndicatorMapPartitionFunction()

        when:
        def mappedEventsIterator = mappingFunction.call(iterator)

        then:
        assert mappedEventsIterator.next().getValidityIndicator() == isValidInd

        where:
        event_id | event_type | event_date                || isValidInd
        111      | "aa"       | "2020-02-20 20:20:20"     || 1
        "aaa"    | "aa"       | "2020-02-20 20:20:20"     || 0
        null     | "aa"       | "2020-02-20 20:20:20"     || 0
        111      | ""         | "2020-02-20 20:20:20"     || 0
        111      | "aa"       | "2020-02-20 20:20:20.562" || 0
        111      | "aa"       | "2020-02-20 20:20:"       || 0
        111      | "aa"       | "2020-02-20"              || 0
        111      | "aa"       | ""                        || 0
        111      | "aa"       | null                      || 0
    }

    @Unroll
    def "FMapping records with parts missing in JSON"() {

        given:
        //prepare data
        def goodRow = "{\"event_id\":222,\"event_type\":\"et1\",\"event_date\":\"2020-02-20 20:20:20\"}"
        def badRow1 = "{\"event_type\":\"et1\",\"event_date\":\"2020-02-20 20:20:20\"}"
        def badRow2 = "{\"event_id\":222,\"event_date\":\"2020-02-20 20:20:20\"}"
        def badRow3 = "{\"event_id\":222,\"event_type\":\"et1\"}"

        def list = new LinkedList<String>()
        list.add(goodRow)
        list.add(badRow1)
        list.add(badRow2)
        list.add(badRow3)
        def iterator = list.iterator()

        def mappingFunction = new RawEventsToValidityIndicatorMapPartitionFunction()

        when:
        def mappedEventsIterator = mappingFunction.call(iterator)

        then:
        assert mappedEventsIterator.toList().get(rowIndex).getValidityIndicator() == isValidInd

        where:
        rowIndex || isValidInd
        0        || 1
        1        || 0
        2        || 0
        3        || 0
    }
}