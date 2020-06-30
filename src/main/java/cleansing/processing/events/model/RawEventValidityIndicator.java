package cleansing.processing.events.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * Class representing mapping of raw event to validity indicator
 */
@Data
@AllArgsConstructor
public class RawEventValidityIndicator {
    String rawEvent;

    //@Min and @Max range: [0 1]
    @Min(value = SchemaConstants.INVALID_EVENT, message = "ValidityIndicator should be 0 (invalid) or 1 (valid)")
    @Max(value = SchemaConstants.VALID_EVENT, message = "ValidityIndicator should be 0 (invalid) or 1 (valid)")
    int validityIndicator;
}
