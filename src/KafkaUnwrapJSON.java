import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Reads Kafka messages with a simple JSON message encapsulating another message
 * as value, then writes the unwrapped value into another message in a different
 * topic. The value itself can be JSON or another text format. The input message
 * would have the body {"name":"value"}, the target has the value as body. The
 * attribute name is not relevant, all attribute after the first one are
 * ignored. The key of the target message can either be empty or the value
 * itself. Header values are not passed. For Kafka, SASL-SSL or no
 * authentication are supported. An empty user name will indicate no
 * authentication.
 * 
 * @author akotopou
 *
 */

public class KafkaUnwrapJSON {

	public static Properties props = new Properties();
	public static boolean debugOutput = true;

	/**
	 * Unwrap value string from JSON string.
	 * 
	 * @param jsonString
	 * @return
	 */
	public static String unwrapJSON(String jsonString) {

		String value = "";

		try {
			JsonFactory jsonFactory = new JsonFactory();
			JsonParser parser = jsonFactory.createParser(jsonString);
			JsonToken startToken = parser.nextToken();
			while (parser.nextToken() != JsonToken.END_OBJECT) {
				if (parser.nextToken() == JsonToken.VALUE_STRING) {
					value = parser.getValueAsString();
					break;
				}
			}

		} catch (Exception jex) {
			System.err.println("Exception during JSONParse:" + jex);
		}

		return value;
	}

	/**
	 * Main method; command line arguments are ignored.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		String bootstrap = "";
		String inTopic = "";
		String outTopic = "m";
		String user = "";
		String password = "";
		boolean valueAsKey = false;

		try {
			FileInputStream in = new FileInputStream("KafkaUnwrapJSON.properties");
			props.load(in);
			in.close();
			bootstrap = props.getProperty("bootstrap");
			user = props.getProperty("user");
			password = props.getProperty("password");
			inTopic = props.getProperty("inTopic");
			outTopic = props.getProperty("outTopic");
			valueAsKey = "true".equals(props.getProperty("valueAsKey", "false"));
			debugOutput = "true".equals(props.getProperty("debugOutput", "false"));
		} catch (Exception ex) {
			System.err.println("Properties file KafkaUnwrapJSON.properties required in current directory.");
			System.exit(1);
		}

		KafkaReader kReader = new KafkaReader(bootstrap, user, password, inTopic, debugOutput);
		KafkaWriter kWriter = new KafkaWriter(bootstrap, user, password, outTopic, debugOutput);

		while (true) {
			List<String> resultList = kReader.getMessages();
			for (String inResult : resultList) {
				if (debugOutput) System.out.println(inResult);
				String outResult = unwrapJSON(inResult);

				if (debugOutput) System.out.println(outResult);
				kWriter.writeMessage(outResult, valueAsKey);
			}
		}
	}

}
