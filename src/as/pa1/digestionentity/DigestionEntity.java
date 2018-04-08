package as.pa1.digestionentity;

import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

/**
 *
 * Interface that specifies DigestionEntity methods and behavior.
 * <p>
 * DigestionEntity should create a KafkaConsumer and KafkaProducer
 * with specific object message each, read messages from Kafka
 * with created KafkaConsumer and send messages to Kafka
 * with created KafkaProducer.
 * 
 * @param <MC>  an object to be received by DigestionEntity
 * @param <MP>  an object to be sent by DigestionEntity
 */
public interface DigestionEntity<MC, MP> {
    
    /**
     *
     * Creates a Kafka consumer for specific object message
     * (object to be received MC)
     * 
     * @return      the Kafka consumer
     * @see         Consumer
     */
    Consumer<Long, MC> createConsumer();
    
    /**
     *
     * Creates a Kafka producer for specific object message
     * (object to be sent MP)
     * 
     * @return      the Kafka producer
     * @see         Producer
     */
    Producer<Long, MP> createProducer();
    
    /**
     *
     * Open Kafka consumer and producer
     * Receive object messages (MC) from Kafka (KafkaConsumer)
     * Create object messages (MP) to send to Kafka
     * Send object messages (MP) to Kafka (KafkaProducer)
     * Close Kafka consumer and producer
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    void runDigestionEntity()
            throws InterruptedException, ExecutionException;
}
