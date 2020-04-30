package cleansing;

import lombok.extern.slf4j.Slf4j;
import cleansing.processing.events.EventsProcessor;

@Slf4j
public class Exercise {

	public static void main(String[] args) {

		log.info("Starting main program");

		if (args == null || args.length == 0 || args.length < 3 ||
				args[0] == null || args[0].isEmpty() ||
				args[1] == null || args[1].isEmpty() ||
				args[2] == null || args[2].isEmpty()) {
			log.error("Mandatory parameters are missing or empty");
			log.error("Use the following pattern to execute the program:");
			log.error("MAIN-CLASS-FULLY-QUALIFIED-NAME [INPUT_DIRECTORY_PATH] [OUTPUT_DIRECTORY_PATH] [ERRORS_DIRECTORY_PATH]");
			throw new IllegalArgumentException("Input parameters requirements are not met!");
		}

		String inputPath = args[0];
		String outputPath = args[1];
		String errorPath = args[2];

		log.info("Initiating data processing");
		EventsProcessor eventsProcessor = new EventsProcessor();
		eventsProcessor.process(inputPath, outputPath, errorPath);
	}
}
