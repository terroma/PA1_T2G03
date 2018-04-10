package as.pa1.digestionentity;

import as.pa1.data.objets.EnrichedStatus;
import as.pa1.data.objets.Status;
import as.pa1.gui.DigestionEntityGUI;
import as.pa1.serialization.EnrichedStatusSerializer;
import as.pa1.serialization.StatusDeserializer;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
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

/**
 *
 * @author Bruno Assunção 89010
 * @author Hugo Chaves  90842
 * 
 */

public class DigestionEntityStatus implements DigestionEntity<Status, EnrichedStatus> {
    
    private String reg = "XX-YY-";
    private final static String ENRICHTOPIC = "EnrichTopic_3";
    private final static String ENRICHEDTOPIC = "EnrichedTopic_3";
    private final static String CLIENT_ID = "DigestionEntitySTATUS";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092, localhost:9093, localhost:9094";
    private DigestionEntityGUI guiFrame;
    
    public DigestionEntityStatus() {
        
    }
    
    public DigestionEntityStatus(DigestionEntityGUI guiFrame) {
        this.guiFrame = guiFrame;
    }
    
    @Override
    public Consumer<Long, Status> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StatusDeserializer.class.getName());
        Consumer<Long, Status> consumer = new KafkaConsumer<>(props);
        //consumer.subscribe(Collections.singleton(ENRICHTOPIC));
        consumer.subscribe(Arrays.asList(ENRICHTOPIC));
        return consumer;
    }
    
    @Override
    public Producer<Long, EnrichedStatus> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EnrichedStatusSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }
    
    @Override
    public void runDigestionEntity() {
        long time = System.currentTimeMillis();
        Consumer<Long, Status> consumer = createConsumer();
        Producer<Long, EnrichedStatus> producer = createProducer();
        try {
            while (true) {
                ConsumerRecords<Long, Status> records = consumer.poll(100);
                if (records.count() != 0) {
                    for (ConsumerRecord<Long, Status> record : records) {
                        if (record.value() == null) {
                            System.out.println("Status recieved as null.");
                        } else {
                            String car_reg = reg+String.format("%02d",record.value().getCar_id());
                            EnrichedStatus enrichedSTATUS = new EnrichedStatus(
                                    record.value().getCar_id(),
                                    record.value().getTime(),
                                    car_reg,
                                    record.value().getMsg_id(),
                                    record.value().getCar_status()
                            );
                            producer.send(new ProducerRecord<Long, EnrichedStatus>(ENRICHEDTOPIC,time,enrichedSTATUS)).get();
                            if (guiFrame != null) {
                                guiFrame.updateStatusText(
                                        record.value().toString(),
                                        enrichedSTATUS.toString());
                            }
                        }
                        time++;
                    }
                }
            }
        } catch (ExecutionException | InterruptedException ex) {
            Logger.getLogger(DigestionEntityStatus.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        } finally {
            consumer.close();
            producer.flush();
            producer.close();
        }
    }
    
    public static void main(String[] args) {
        DigestionEntityStatus des = new DigestionEntityStatus();
        des.runDigestionEntity();
    }
}
