import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;


public class SingleConsumerMultipleTopic {

	public static void main(String[] args) {
		
		String topicName = "Neva-Topic";
		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);


		ConsumerRecords<String, String> records = consumer.poll(10);
		for (TopicPartition partition : records.partitions()) 
		{
		    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
		    for (ConsumerRecord<String, String> record : partitionRecords) 
		    {
		        System.out.println(record.offset() + ": " + record.value());
		    }
		    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
		    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
		}

	}

}
