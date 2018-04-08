package as.pa1.collectentity;

import as.pa1.data.objets.Speed;
import as.pa1.serialization.SpeedSerializer;
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

public class CollectEntitySpeed implements CollectEntity<Speed> {
    
    private static final String PATH = new File("").getAbsolutePath().concat("/src/as/pa1/data/SPEED.txt");
    private static final String CLIENT_ID = "CollectEntitySPEED";
    private static final String TOPIC = "EnrichTopic_2";
    private static final String BOOTSTRAP_SERVERS = 
            "loaclhost:9092,loacalhost:9093,localhost:9094";
    private BufferedReader in;
    
    @Override
    public Producer<Long, Speed> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpeedSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        //props.put("max.inflight.messages", 1);
        return new KafkaProducer<>(props);
    }
    
    @Override
    public void runCollectEntity() {
        long time = System.currentTimeMillis();
        Producer<Long, Speed> producer = createProducer();
        
        try {
            in = new BufferedReader(new FileReader(PATH));
            String line;
            long index = time;
            
            while ((line = in.readLine()) != null) {
                String[] lineArgs = line.split("\\|");
                Speed spd = new Speed(Integer.parseInt(lineArgs[0]),Integer.parseInt(lineArgs[1]),lineArgs[2],Integer.parseInt(lineArgs[3]),Integer.parseInt(lineArgs[4]));
                producer.send(new ProducerRecord<Long, Speed>(TOPIC,index,spd)).get();
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
            Logger.getLogger(CollectEntitySpeed.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        } catch (IOException ex) {
            Logger.getLogger(CollectEntitySpeed.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(CollectEntitySpeed.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        } catch (ExecutionException ex) {
            Logger.getLogger(CollectEntitySpeed.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        } finally {
            producer.flush();
            producer.close();
        }
        
    }
    
    public static void main(String[] args) {
        CollectEntitySpeed cespeed = new CollectEntitySpeed();
        cespeed.runCollectEntity();
    }
    
}
