#Utility to copy messages from one Kafka topic to another while unwrapping value from JSON body

Reads Kafka messages with a simple JSON message encapsulating another message as value, then writes the unwrapped value into another message in a different mtopic. The value itself can be JSON or another text format. The input message would have the body `{"name":"value"}`, the target has the `value` as body. The attribute name is not relevant, all attribute after the first one areignored. The key of the target message can either be empty or the valueitself. Header values are not passed. For Kafka, SASL-SSL or no authentication are supported. An empty user name will indicate no authentication. 

Execution:
`java -jar ./target/KafkaUnwrapJSON-0.0.1-SNAPSHOT.jar`

Parameters need to be set in KafkaUnwrapJSON.properties file in execution directory:
* **bootstrap** - Hostname and port of Kafka bootstrap server, for example 10.1.1.234:9092
* **user** - User for Kafka , leave empty for no authentication. Supports OCI Streaming format tenancy_name/user_name/streampool_ocid
* **password** - Password for SASL authentication. Supports OCI Streaming format with authtoken as password
* **inTopic** - Topic for input messages. Needs to be created prior to execution
* **outTopic** - Topic for output messages. Needs to be created prior to execution
* **keyAsValue** - If true, output message key is populated with full message body, otherwise left empty.
* **debugOutput** - If true, input and output messages are printed on STDOUT. If false, no debug output is printed