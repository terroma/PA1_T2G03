package as.pa1.digestionentity;

import as.pa1.data.objets.HeartBeat;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class DigestionEntityHB {
    
    private final static String ENRICHTOPIC = "test";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092, localhost:9093, localhost:9094";
    
    private static Consumer<Long, HeartBeat> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DigestionEntityHB");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "as.pa1.serialization.HeartBeatDeserializer");
//      props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        Consumer<Long, HeartBeat> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(ENRICHTOPIC));
        return consumer;
    }
    
    private void runConsumer() throws InterruptedException {
        
        Consumer<Long, HeartBeat> consumer = createConsumer();
        final int giveUp = 100;
        int noRecordsCount = 0;
        try {
            while (true) {
                ConsumerRecords<Long, HeartBeat> records = consumer.poll(100);
                for (ConsumerRecord<Long, HeartBeat> record : records) {
                    HeartBeat a = record.value();
                    System.out.println("HeartBeatTime: "+record.value().getTime());
                    System.out.println("HeartBeatTimeFromA: "+a.getTime());
                }
                /**
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
                **/
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            //consumer.commitSync();
            consumer.close();
        }  
    }
    
    public static void main(String[] args) {
        DigestionEntityHB dhb = new DigestionEntityHB();
        try {
            dhb.runConsumer();
        } catch (InterruptedException ex) {
            Logger.getLogger(DigestionEntityHB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
