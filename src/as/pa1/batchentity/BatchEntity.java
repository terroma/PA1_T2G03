package as.pa1.batchentity;

import as.pa1.data.objets.EnrichedHeartBeat;
import as.pa1.data.objets.EnrichedSpeed;
import as.pa1.data.objets.EnrichedStatus;
import as.pa1.gui.BatchEntityGUI;
import as.pa1.serialization.EnrichedHeartBeatDeserializer;
import as.pa1.serialization.EnrichedSpeedDeserializer;
import as.pa1.serialization.EnrichedStatusDeserializer;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

/**
 *
 * @author Bruno Assunção 89010
 * @author Hugo Chaves  90842
 * 
 */

public class BatchEntity {
    private static final String PATH = new File("").getAbsolutePath().concat("/src/as/pa1/data/BATCH.txt");
    private static final String CLIENT_ID = "BatchEntity";
    private static final String[] TOPICS = {"EnrichedTopic_1","EnrichedTopic_2","EnrichedTopic_3"};
    private static final String BOOTSTRAP_SERVERS = 
            "loaclhost:9092,loacalhost:9093,localhost:9094";
    private BufferedWriter out;
    private BatchEntityGUI guiFrame;
    
    public BatchEntity() {
        
    }
    
    public BatchEntity(BatchEntityGUI guiFrame) {
        this.guiFrame = guiFrame;
    }
    
    private Consumer<Long, EnrichedHeartBeat> createHeartBeatConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID+"1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EnrichedHeartBeatDeserializer.class.getName());
        Consumer<Long, EnrichedHeartBeat> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPICS[0]));
        return consumer;
    }
    
    private Consumer<Long, EnrichedSpeed> createSpeedConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID+"2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EnrichedSpeedDeserializer.class.getName());
        Consumer<Long, EnrichedSpeed> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPICS[1]));
        return consumer;
    }
    
    private Consumer<Long, EnrichedStatus> createStatusConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID+"3");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EnrichedStatusDeserializer.class.getName());
        Consumer<Long, EnrichedStatus> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPICS[2]));
        return consumer;
    }
    //TODO try better implementation
    public void runBatchEntity() {
        long time = System.currentTimeMillis();
        Consumer<Long, EnrichedHeartBeat> heartbeatConsumer = createHeartBeatConsumer();
        Consumer<Long, EnrichedSpeed> speedConsumer = createSpeedConsumer();
        Consumer<Long, EnrichedStatus> statusConsumer = createStatusConsumer();
        try {
            String line = null;
            // empty file before writing to it
            out = new BufferedWriter(new FileWriter(PATH));
            out.close();
            
            while (true) {
                ConsumerRecords<Long, EnrichedHeartBeat> heartbeatRecords = heartbeatConsumer.poll(100);
                if (heartbeatRecords.count() != 0) {
                    out = new BufferedWriter(new FileWriter(PATH,true));
                    for (ConsumerRecord<Long, EnrichedHeartBeat> heartbeatRecord : heartbeatRecords) {
                        if (heartbeatRecord.value() != null ) {
                            line = heartbeatRecord.value().toString();
                            System.out.println("Writing EnrichedHeartBeat: " + line);
                            out.write(line);
                            out.newLine();
                            if (guiFrame != null)
                                guiFrame.updateBatchEntityText(line);
                        }
                    }
                    //out.flush();
                    out.close();
                }
                ConsumerRecords<Long, EnrichedSpeed> speedRecords = speedConsumer.poll(100);
                if (speedRecords.count() != 0) {
                    out = new BufferedWriter(new FileWriter(PATH,true));
                    for (ConsumerRecord<Long, EnrichedSpeed> speedRecord : speedRecords) {
                        if (speedRecord.value() != null) {
                            line = speedRecord.value().toString();
                            System.out.println("Writing EnrichedSpeed: " + line);
                            out.write(line);
                            out.newLine();
                            if (guiFrame != null)
                                guiFrame.updateBatchEntityText(line);
                        }
                    }
                    //out.flush();
                    out.close();
                }
                ConsumerRecords<Long, EnrichedStatus> statusRecords = statusConsumer.poll(100);
                if (statusRecords.count() != 0) {
                    out = new BufferedWriter(new FileWriter(PATH,true));
                    for (ConsumerRecord<Long, EnrichedStatus> statusRecord: statusRecords) {
                        if (statusRecord.value() != null) {
                            line = statusRecord.value().toString();
                            System.out.println("Writing EnrichedStatus: " + line);
                            out.write(line);
                            out.newLine();
                            if (guiFrame != null)
                                guiFrame.updateBatchEntityText(line);
                        }  
                    }
                    //out.flush();
                    out.close();
                }
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(BatchEntity.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        } catch (IOException ex) {
            Logger.getLogger(BatchEntity.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        } finally {
            heartbeatConsumer.close();
            speedConsumer.close();
            statusConsumer.close();
        }
        
    }
    
    public static void main(String[] args) {
        BatchEntity be = new BatchEntity();
        be.runBatchEntity();
    }
}
