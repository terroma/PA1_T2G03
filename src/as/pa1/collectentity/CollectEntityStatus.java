package as.pa1.collectentity;

import as.pa1.data.objets.Status;
import as.pa1.gui.CollectEntityGUI;
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
import org.apache.kafka.common.serialization.LongSerializer;

/**
 *
 * @author Bruno Assunção 89010
 * @author Hugo Chaves  90842
 * 
 */

public class CollectEntityStatus implements CollectEntity<Status> {
    
    private static final String PATH = new File("").getAbsolutePath().concat("/src/as/pa1/data/STATUS.txt");
    private static final String CLIENT_ID = "CollectEntitySTATUS";
    private static final String TOPIC = "EnrichTopic_3";
    private static final String BOOTSTRAP_SERVERS = 
            "loaclhost:9092,loacalhost:9093,localhost:9094";
    private BufferedReader in;
    private CollectEntityGUI guiFrame;
    
    public CollectEntityStatus() {
        
    }
    
    public CollectEntityStatus(CollectEntityGUI guiFrame) {
        this.guiFrame = guiFrame;
    }
    
    @Override
    public Producer<Long, Status> createProducer() {
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
    
    @Override
    public void runCollectEntity() {
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
                if (guiFrame != null) {
                    Thread.sleep(1000);
                    guiFrame.updateStatusText(st.toString());
                }
                index++;    
            }
            in.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(CollectEntityStatus.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        } catch (IOException ex) {
            Logger.getLogger(CollectEntityStatus.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(CollectEntityStatus.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        } catch (ExecutionException ex) {
            Logger.getLogger(CollectEntityStatus.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        } finally {
            producer.flush();
            producer.close();
        }
        
    }
    
    public static void main(String[] args) {
        CollectEntityStatus cestatus = new CollectEntityStatus();
        cestatus.runCollectEntity();
    }
    
}
