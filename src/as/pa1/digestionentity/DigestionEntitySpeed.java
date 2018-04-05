package as.pa1.digestionentity;

import as.pa1.data.objets.EnrichedSpeed;
import as.pa1.data.objets.Speed;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

public class DigestionEntitySpeed {
    
    private String reg = "XX-YY-";
    private final static int MAX_SPEED = 100;
    private final static String ENRICHTOPIC = "EnrichTopic_2";
    private final static String ENRICHEDTOPIC = "EnrichedTopic_2";
    private final static String CLIENT_ID = "DigestionEntitySPEED";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092, localhost:9093, localhost:9094";
    
    private Consumer<Long, Speed> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "as.pa1.serialization.SpeedDeserializer");
        Consumer<Long, Speed> consumer = new KafkaConsumer<>(props);
        //consumer.subscribe(Collections.singleton(ENRICHTOPIC));
        consumer.subscribe(Arrays.asList(ENRICHTOPIC));
        return consumer;
    }
    
    private Producer<Long, EnrichedSpeed> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "as.pa1.serialization.EnrichedSpeedSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");       
        return new KafkaProducer<>(props);
    }
    
    private void runDigestionEntitySpeed() throws InterruptedException {
        long time = System.currentTimeMillis();
        Consumer<Long, Speed> consumer = createConsumer();
        Producer<Long, EnrichedSpeed> producer = createProducer();
        
        try {
            while (true) {
                ConsumerRecords<Long, Speed> records = consumer.poll(100);
                if (records.count() != 0) {
                    for (ConsumerRecord<Long, Speed> record : records) {
                        if (record.value() == null) {
                            System.out.println("Speed recieved as null.");
                        } else {
                            String car_reg = reg+String.format("%02d", record.value().getCar_id());
                            System.out.println(car_reg);
                            EnrichedSpeed enrichedSPEED = new EnrichedSpeed(
                                    record.value().getCar_id(),
                                    record.value().getTime(),
                                    car_reg,
                                    record.value().getMsg_id(),
                                    record.value().getSpeed(),
                                    record.value().getLocalization(),
                                    MAX_SPEED
                            );
                            producer.send(new ProducerRecord<Long, EnrichedSpeed>(ENRICHEDTOPIC,time,enrichedSPEED)).get();
                        }
                        time++;
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            consumer.close();
            producer.flush();
            producer.close();
        }
    }
    
    public static void main(String[] args) {
        DigestionEntitySpeed des = new DigestionEntitySpeed();
        try {
            des.runDigestionEntitySpeed();
        } catch (InterruptedException ex) {
            Logger.getLogger(DigestionEntitySpeed.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}