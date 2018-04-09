package as.pa1.alarmentity;

import as.pa1.data.objets.EnrichedSpeed;
import as.pa1.serialization.EnrichedSpeedDeserializer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

public class AlarmEntity {
    
    private final static String TOPIC = "EnrichedTopic_2";
    private final static String CLIENT_ID = "AlarmEntity";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092, localhost:9093, localhost:9094";
    private HashMap<Integer, Alarm> lastSpeedMap;
    
    public AlarmEntity() {
        lastSpeedMap = new HashMap<>();
    }
    
    private Consumer<Long, EnrichedSpeed> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EnrichedSpeedDeserializer.class.getName());
        Consumer<Long, EnrichedSpeed> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC));
        return consumer;
    }
    
    /**
     * ON  && speed > max_speed     ---> ON
     * ON  && speed <= max_speed    ---> OFF
     * OFF && speed > max_speed     ---> ON
     * OFF && speed <= max_speed    ---> OFF
     * 
     * @param car_id
     * @param speed
     * @param max_speed 
     */
    private boolean stateChanged(int car_id, int speed, int max_speed) {
        if (lastSpeedMap.get(car_id) == Alarm.ON 
                && speed <= max_speed) {
            lastSpeedMap.replace(car_id, Alarm.OFF);
            return true;
        }
        if (lastSpeedMap.get(car_id) == Alarm.OFF
                && speed > max_speed) {
            lastSpeedMap.replace(car_id, Alarm.ON);
            return true;
        }
        return false;
    }
    
    private void checkSpeed(int car_id, int speed, int max_speed) {
        if (speed > max_speed) {
            lastSpeedMap.put(car_id, Alarm.ON);
        } else {
            lastSpeedMap.put(car_id, Alarm.OFF);
        }
    }
    
    public void runAlarmEntity() {
        long time = System.currentTimeMillis();
        Consumer<Long, EnrichedSpeed> consumer = createConsumer();
        String line = null;
        
        try {
            while (true) {
                ConsumerRecords<Long, EnrichedSpeed> records = consumer.poll(100);
                if (records.count() != 0) {
                    for (ConsumerRecord<Long, EnrichedSpeed> record : records) {
                        if (record.value() == null) {
                            System.out.println("EnrichedSpeed recieved as null.");
                        } else {
                            EnrichedSpeed enrSpeed = record.value();
                            // compare last speed with current speed and turn on or off alarm
                            if (!lastSpeedMap.containsKey(enrSpeed.getCar_id())) {
                                checkSpeed(enrSpeed.getCar_id(), enrSpeed.getSpeed(), enrSpeed.getMax_speed());
                            } else {
                                if (stateChanged(enrSpeed.getCar_id(), enrSpeed.getSpeed(), enrSpeed.getMax_speed())) {
                                    line = enrSpeed.toString()+"|"+lastSpeedMap.get(enrSpeed.getCar_id()).getValue();
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
        }
    }
    
    public static void main(String[] args) {
        AlarmEntity ae = new AlarmEntity();
        ae.runAlarmEntity();
    }
}
