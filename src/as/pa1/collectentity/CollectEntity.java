package as.pa1.collectentity;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Producer;

/**
 *
 * Interface that specifies CollectEntity methods and behavior.
 * <p>
 * CollectEntity should create a KafkaProducer with specific
 * object message, read file line by line and send message
 * to Kafka with created KafkaProducer.
 * 
 * @param <M>   an object to be sent by CollectEntity
 */
public interface CollectEntity<M> {
    
    /**
     * 
     * Creates a Kafka producer for specific object message
     * (object to be sent M)
     * 
     * @return      the Kafka producer
     * @see         Producer
     */
    Producer<Long, M> createProducer();
    
    /**
     *
     * Read file line by line
     * Create object message (M)
     * Send object message (M) to Kafka
     * Close file and Kafka producer
     * 
     * @throws java.io.FileNotFoundException
     * @throws IOException
     * @throws java.lang.InterruptedException
     * @throws java.util.concurrent.ExecutionException
     */
    void runCollectEntity()
            throws FileNotFoundException, IOException, InterruptedException, ExecutionException;
}
