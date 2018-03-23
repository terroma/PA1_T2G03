package as.pa1.digestionentity;

import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class DigestionEntityHB {
    
    private final static String ENRICHTOPIC = "EnrichTopic_1";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092, localhost:9093, localhost:9094";
    
    private static Consumer<Long, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DigestionEntityHB");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//      props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(ENRICHTOPIC));
        return consumer;
    }
    
    private void runConsumer() throws InterruptedException {
        
        final Consumer<Long, String> consumer = createConsumer();
        final int giveUp = 100;
        int noRecordsCount = 0;
        try {
            while (true) {
                final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

                if (consumerRecords.count() == 0) {
                   noRecordsCount++;

                   if (noRecordsCount > giveUp) break;
                   else continue;
                }

                consumerRecords.forEach(record -> {
                    System.out.printf("Consumer Record:(key=%d value=%s partition=%d offset=%d)\n",
                            record.key(), record.value(),
                            record.partition(), record.offset());
                });
                consumer.commitAsync();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            //consumer.commitSync();
            consumer.close();
        }  
    }
    
    public static void main(String[] args) {
        //TODO
    }
}
