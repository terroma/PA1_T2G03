package as.pa1.collectentity;

import as.pa1.data.objets.HeartBeat;
import as.pa1.gui.CollectEntityGUI;
import as.pa1.serialization.HeartBeatSerializer;
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
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class CollectEntityHeartBeat {
    
    private static final String PATH = new File("").getAbsolutePath().concat("/src/as/pa1/data/HB.txt");
    private static final String CLIENT_ID = "CollectEntityHB";
    private static final String TOPIC = "EnrichTopic_1";
    private static final String BOOTSTRAP_SERVERS = 
            "loaclhost:9092,loacalhost:9093,localhost:9094";
    private BufferedReader in;
    private CollectEntityGUI guiFrame;
    
    public CollectEntityHeartBeat() {
        
    }
    
    public CollectEntityHeartBeat(CollectEntityGUI guiFrame) {
        this.guiFrame = guiFrame;
    }
    
    private Producer<Long, HeartBeat> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, HeartBeatSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        return new KafkaProducer<>(props);
    }
    
    public void runProducer() {
        long time = System.currentTimeMillis();
        Producer<Long, HeartBeat> producer = createProducer();
        
        try {
            in = new BufferedReader(new FileReader(PATH));
            String line;
            
            while ((line = in.readLine()) != null) {
                String[] lineArgs = line.split("\\|");
                HeartBeat hb = new HeartBeat(Integer.parseInt(lineArgs[0]),Integer.parseInt(lineArgs[1]),lineArgs[2]);
                producer.send(new ProducerRecord<Long, HeartBeat>(TOPIC,time,hb)).get();
                // sleep for showing purposes
                //Thread.sleep(1000);
                guiFrame.updateHeartBeatText(line);
                /**
                final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC,line);
                producer.send(record);
                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) time=%d\n",
                                    record.key(), record.value(), elapsedTime);
                **/
            }
            in.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(CollectEntityHeartBeat.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(CollectEntityHeartBeat.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(CollectEntityHeartBeat.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ExecutionException ex) {
            Logger.getLogger(CollectEntityHeartBeat.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            producer.flush();
            producer.close();
        }
        
    }
    
    public static void main(String[] args) {
        CollectEntityHeartBeat cehb = new CollectEntityHeartBeat();
        cehb.runProducer();
    }
    
}
