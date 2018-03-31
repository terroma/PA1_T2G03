package as.pa1.digestionentity;

import as.pa1.data.objets.EnrichedHeartBeat;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class DigestionEntityHeartBeat {
    
    private String reg = "XX-YY-";
    private final static String ENRICHTOPIC = "EnrichTopic_1";
    private final static String ENRICHEDTOPIC = "EnrichedTopic_1";
    private final static String CLIENT_ID = "DigestionEntityHB";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092, localhost:9093, localhost:9094";
    
    private Consumer<Long, HeartBeat> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "as.pa1.serialization.HeartBeatDeserializer");
//      props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        Consumer<Long, HeartBeat> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(ENRICHTOPIC));
        return consumer;
    }
    
    private Producer<Long, EnrichedHeartBeat> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "as.pa1.serialization.EnrichedHeartBeatSerializer");
        props.put(ProducerConfig.ACKS_CONFIG,"0");
        return new KafkaProducer<>(props);
    }
    
    private void runDigestionEntityHeartBeat() throws InterruptedException {
        long time = System.currentTimeMillis();
        Consumer<Long, HeartBeat> consumer = createConsumer();
        Producer<Long, EnrichedHeartBeat> producer = createProducer();
        final int giveUp = 100;
        int noRecordsCount = 0;
        
        try {
            while (true) {
                ConsumerRecords<Long, HeartBeat> records = consumer.poll(100);
                if (records.count() != 0) {
                    for (ConsumerRecord<Long, HeartBeat> record : records) {
                        if (record.value() == null) {
                            System.out.println("HeartBeat recieved as null.");
                        } else {
                            String car_reg = reg+String.format("%02d", record.value().getCar_id());
                            System.out.println(car_reg);
                            EnrichedHeartBeat enrichedHB = new EnrichedHeartBeat(
                                    record.value().getCar_id(),
                                    record.value().getTime(),
                                    car_reg,
                                    record.value().getMsg_id()
                            );
                            producer.send(new ProducerRecord<Long, EnrichedHeartBeat>(ENRICHEDTOPIC,time,enrichedHB)).get();
                        }
                        time++;
                    }
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
            producer.flush();
            producer.close();
        }  
    }
    
    public static void main(String[] args) {
        DigestionEntityHeartBeat dhb = new DigestionEntityHeartBeat();
        try {
            dhb.runDigestionEntityHeartBeat();
        } catch (InterruptedException ex) {
            Logger.getLogger(DigestionEntityHeartBeat.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
