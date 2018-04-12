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

/**
 *
 * @author Bruno Assunção 89010
 * @author Hugo Chaves  90842
 * 
 */

public class CollectEntityHeartBeat implements CollectEntity<HeartBeat> {
    
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
    
    @Override
    public Producer<Long, HeartBeat> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, HeartBeatSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        return new KafkaProducer<>(props);
    }
    
    @Override
    public void runCollectEntity() {
        long time = System.currentTimeMillis();
        Producer<Long, HeartBeat> producer = createProducer();
        
        try {
            in = new BufferedReader(new FileReader(PATH));
            String line;
            long index = time;
            
            while ((line = in.readLine()) != null) {
                String[] lineArgs = line.split("\\|");
                HeartBeat hb = new HeartBeat(Integer.parseInt(lineArgs[0]),Integer.parseInt(lineArgs[1]),lineArgs[2]);
                producer.send(new ProducerRecord<Long, HeartBeat>(TOPIC, index, hb));
                if (guiFrame != null) {
                    // sleep for showing purposes
                    Thread.sleep(1000);
                    guiFrame.updateHeartBeatText(hb.toString());
                }
                index++;
            }
            in.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(CollectEntityHeartBeat.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        } catch (IOException ex) {
            Logger.getLogger(CollectEntityHeartBeat.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(CollectEntityHeartBeat.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        } finally {
            producer.flush();
            producer.close();
        }
        
    }
    
    public static void main(String[] args) {
        CollectEntityHeartBeat cehb = new CollectEntityHeartBeat();
        cehb.runCollectEntity();
    }
    
}
