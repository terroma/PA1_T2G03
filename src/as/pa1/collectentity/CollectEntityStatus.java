package as.pa1.collectentity;

import as.pa1.data.objets.Status;
import as.pa1.serialization.StatusSerializer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class CollectEntityStatus {
    
    private static final String PATH = new File("").getAbsolutePath().concat("/src/as/pa1/data/STATUS.txt");
    private static final String CLIENT_ID = "CollectEntitySTATUS";
    private static final String TOPIC = "EnrichTopic_3";
    private static final String BOOTSTRAP_SERVERS = 
            "loaclhost:9092,loacalhost:9093,localhost:9094";
    private BufferedReader in;
    
    private Producer<Long, Status> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StatusSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put("max.inflight.messages", 1);
        return new KafkaProducer<>(props);
    }
    
    private void runProducer() {
        long time = System.currentTimeMillis();
        Producer<Long, Status> producer = createProducer();
        
        try {
            in = new BufferedReader(new FileReader(PATH));
            String line;
            long index = time;
            
            while ((line = in.readLine()) != null) {
                String[] lineArgs = line.split("\\|");
                Status st = new Status(Integer.parseInt(lineArgs[0]),Integer.parseInt(lineArgs[1]),lineArgs[2],lineArgs[3]);
                producer.send(new ProducerRecord<Long, Status>(TOPIC,index,st)).get();
                /**
                final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, index, line);
                RecordMetadata metadata = producer.send(record).get();
                
                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s)" +
                                "meta(partition=%d offset=%d) time=%d\n",
                                record.key(), record.value(),
                                metadata.partition(), metadata.offset(), elapsedTime);
                **/
                index++;    
            }
            in.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(CollectEntityHeartBeat.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(CollectEntityHeartBeat.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException | ExecutionException ex) {
            Logger.getLogger(CollectEntitySpeed.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            producer.flush();
            producer.close();
        }
        
    }
    
    public static void main(String[] args) {
        CollectEntityStatus cestatus = new CollectEntityStatus();
        cestatus.runProducer();
    }
    
}
