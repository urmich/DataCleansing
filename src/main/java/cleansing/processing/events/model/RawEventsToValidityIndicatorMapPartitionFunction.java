package cleansing.processing.events.model;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.validator.GenericValidator;
import org.apache.spark.api.java.function.MapPartitionsFunction;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Class that implement mapping of all partition events to 1 (valid) or 0 (invalid)
 */
@Slf4j
public class RawEventsToValidityIndicatorMapPartitionFunction implements MapPartitionsFunction<String, RawEventValidityIndicator> {

    private List<RawEventValidityIndicator> rawEventValidityIndicatorList = new LinkedList();

    private DateFormat sdf = new SimpleDateFormat(SchemaConstants.MandatoryDataColumns.EVENT_DATE_FORMAT);

    /**
     * This method attempts to resolve the JSON String and validate event mandatory parameters.
     * If all passes and valid, the @rawEvent string is mapped to 1, otherwise to 0.
     *
     * @param iterator for all entries in partition
     * @return Iterator of list with mapped objects
     * @throws Exception
     */
    public Iterator<RawEventValidityIndicator> call(Iterator<String> iterator) throws Exception {

        int validityIndicator = 0;
        sdf.setLenient(false);
        boolean eventValid = true;

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true);

        while (iterator.hasNext()) {
            String rawEvent = iterator.next();
            Event event = null;
            try {
                //Verifying JSON by parsing it
                event = objectMapper.readValue(rawEvent, Event.class);
                //Verify date
                eventValid = isEventDateValid(event.getDate());
            } catch (Exception e) {
                eventValid = false;
            }

            //keep validating if no validation error occurred till now
            eventValid = eventValid && isEventIdValid(event.getId()) && isEventTypeValid(event.getType());
            validityIndicator = eventValid ? SchemaConstants.VALID_EVENT : SchemaConstants.INVALID_EVENT;

            rawEventValidityIndicatorList.add(new RawEventValidityIndicator(rawEvent, validityIndicator));
        }

        return rawEventValidityIndicatorList.iterator();
    }


    //method that validates date
    private boolean isEventDateValid(String eventDate){
        return GenericValidator.isDate(eventDate, SchemaConstants.MandatoryDataColumns.EVENT_DATE_FORMAT, true);
    }

    //method that verifies that event type is valid
    private boolean isEventTypeValid(String eventType) {
        return !GenericValidator.isBlankOrNull(eventType);
    }

    //method that verifies that event id is valid
    private boolean isEventIdValid(String eventIdStr) {
        return GenericValidator.isInt(eventIdStr);
    }
}
