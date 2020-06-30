package cleansing.processing.events.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Class representing the event
 */
@Data
public class Event {
    @JsonProperty("event_id")
    String id;

    @JsonProperty("event_type")
    String type;

    @JsonProperty("event_date")
    String date;
}
