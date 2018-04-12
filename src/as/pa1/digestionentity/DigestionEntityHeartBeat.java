package as.pa1.digestionentity;

import as.pa1.data.objets.EnrichedHeartBeat;
import as.pa1.data.objets.HeartBeat;
import as.pa1.gui.DigestionEntityGUI;
import as.pa1.serialization.EnrichedHeartBeatSerializer;
import as.pa1.serialization.HeartBeatDeserializer;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
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

/**
 *
 * @author Bruno Assunção 89010
 * @author Hugo Chaves  90842
 * 
 */

public class DigestionEntityHeartBeat implements DigestionEntity<HeartBeat, EnrichedHeartBeat> {
    
    private String reg = "XX-YY-";
    private final static String ENRICHTOPIC = "EnrichTopic_1";
    private final static String ENRICHEDTOPIC = "EnrichedTopic_1";
    private final static String CLIENT_ID = "DigestionEntityHB";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092, localhost:9093, localhost:9094";
    private DigestionEntityGUI guiFrame;
    
    public DigestionEntityHeartBeat() {
        
    }
    
    public DigestionEntityHeartBeat(DigestionEntityGUI guiFrame) {
        this.guiFrame = guiFrame;
    }
    
    @Override
    public Consumer<Long, HeartBeat> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HeartBeatDeserializer.class.getName());
//      props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        Consumer<Long, HeartBeat> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(ENRICHTOPIC));
        return consumer;
    }
    
    @Override
    public Producer<Long, EnrichedHeartBeat> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EnrichedHeartBeatSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,"0");
        return new KafkaProducer<>(props);
    }
    
    @Override
    public void runDigestionEntity() {
        long time = System.currentTimeMillis();
        Consumer<Long, HeartBeat> consumer = createConsumer();
        Producer<Long, EnrichedHeartBeat> producer = createProducer();
        
        try {
            long index = time;
            
            while (true) {
                ConsumerRecords<Long, HeartBeat> records = consumer.poll(100);
                if (records.count() != 0) {
                    for (ConsumerRecord<Long, HeartBeat> record : records) {
                        if (record.value() == null) {
                            System.out.println("HeartBeat recieved as null.");
                        } else {
                            String car_reg = reg+String.format("%02d", record.value().getCar_id());
                            EnrichedHeartBeat enrichedHB = new EnrichedHeartBeat(
                                    record.value().getCar_id(),
                                    record.value().getTime(),
                                    car_reg,
                                    record.value().getMsg_id()
                            );
                            producer.send(new ProducerRecord<Long, EnrichedHeartBeat>(ENRICHEDTOPIC,index,enrichedHB));
                            if (guiFrame != null) {
                                guiFrame.updateHeartBeatText(
                                        record.value().toString(),
                                        enrichedHB.toString());
                            }
                        }
                        index++;
                    }
                }
            }
       } finally {
            consumer.close();
            producer.flush();
            producer.close();
        }  
    }
    
    public static void main(String[] args) {
        DigestionEntityHeartBeat dhb = new DigestionEntityHeartBeat();
        dhb.runDigestionEntity();
    }
}
