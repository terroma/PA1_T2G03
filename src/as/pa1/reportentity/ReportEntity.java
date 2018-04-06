package as.pa1.reportentity;

import as.pa1.data.objets.EnrichedHeartBeat;
import as.pa1.data.objets.EnrichedSpeed;
import as.pa1.data.objets.EnrichedStatus;
import as.pa1.serialization.EnrichedHeartBeatDeserializer;
import as.pa1.serialization.EnrichedSpeedDeserializer;
import as.pa1.serialization.EnrichedStatusDeserializer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongSerializer;

public class ReportEntity {
    private static final String CLIENT_ID = "ReportEntity";
    private static final String[] TOPICS = {"EnrichedTopic_1","EnrichedTopic_2","EnrichedTopic_3"};
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    private ReportEntityDBConnection dbConnection;
    
    private Consumer<Long, EnrichedHeartBeat> createHeartBeatConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EnrichedHeartBeatDeserializer.class.getName());
        Consumer<Long, EnrichedHeartBeat> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPICS[0]));
        return consumer;
    }
    
    private Consumer<Long, EnrichedSpeed> createSpeedConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EnrichedSpeedDeserializer.class.getName());
        Consumer<Long, EnrichedSpeed> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPICS[1]));
        return consumer;
    }
    
    private Consumer<Long, EnrichedStatus> createStatusConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EnrichedStatusDeserializer.class.getName());
        Consumer<Long, EnrichedStatus> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPICS[2]));
        return consumer;
    }
    
    private void runReportEntity() {
        long time = System.currentTimeMillis();
        final int batchSize = 1000;
        int noRecordsCount = 0;
        int count = 0;
        
        Consumer<Long, EnrichedHeartBeat> heartbeatConsumer = createHeartBeatConsumer();
        Consumer<Long, EnrichedSpeed> speedConsumer = createSpeedConsumer();
        Consumer<Long, EnrichedStatus> statusConsumer = createStatusConsumer();
        
        dbConnection = new ReportEntityDBConnection();
        dbConnection.init();
        
        String insertHeartBeat = "insert into enrichedheartbeat (car_id, time, car_reg, msg_id) values (?, ?, ?, ?)";
        String insertSpeed = "insert into enrichedheartbeat (car_id, time, car_reg, msg_id, speed, localization, max_speed) values (?, ?, ?, ?, ?, ?, ?)";
        String insertStatus = "insert into enrichedheartbeat (car_id, time, car_reg, msg_id, car_status) values (?, ?, ?, ?, ?)";
        
        PreparedStatement psHeartBeat = null;
        PreparedStatement psSpeed = null;
        PreparedStatement psStatus = null;
        try {
            psHeartBeat = dbConnection.getConnection().prepareStatement(insertHeartBeat);
            psSpeed = dbConnection.getConnection().prepareStatement(insertSpeed);
            psStatus = dbConnection.getConnection().prepareStatement(insertStatus);
            
            while (true) {
                ConsumerRecords<Long, EnrichedHeartBeat> hbRecords = heartbeatConsumer.poll(100);
                if (hbRecords.count() != 0) {
                    for (ConsumerRecord<Long, EnrichedHeartBeat> hbRecord : hbRecords) {
                        if (hbRecord.value() != null) {
                            EnrichedHeartBeat enrHeartBeat = hbRecord.value();
                            psHeartBeat.setInt(1, enrHeartBeat.getCar_id());
                            psHeartBeat.setInt(2, enrHeartBeat.getTime());
                            psHeartBeat.setString(3, enrHeartBeat.getCar_reg());
                            psHeartBeat.setString(4, enrHeartBeat.getMsg_id());
                            psHeartBeat.addBatch();
                            
                            if (++count % batchSize == 0) 
                                psHeartBeat.executeBatch();
                        }
                    }
                    psHeartBeat.executeBatch();
                }
                ConsumerRecords<Long, EnrichedSpeed> spRecords = speedConsumer.poll(100);
                if (spRecords.count() != 0) {
                    for (ConsumerRecord<Long, EnrichedSpeed> spRecord : spRecords) {
                        if (spRecord.value() != null) {
                            EnrichedSpeed enrSpeed = spRecord.value();
                            psSpeed.setInt(1, enrSpeed.getCar_id());
                            psSpeed.setInt(2, enrSpeed.getTime());
                            psSpeed.setString(3, enrSpeed.getCar_reg());
                            psSpeed.setString(4, enrSpeed.getMsg_id());
                            psSpeed.setInt(5, enrSpeed.getSpeed());
                            psSpeed.setInt(6, enrSpeed.getLocalization());
                            psSpeed.setInt(7, enrSpeed.getMax_speed());
                            psSpeed.addBatch();
                            
                            if (++count % batchSize == 0)
                                psSpeed.executeBatch();
                        }
                    }
                    psSpeed.executeBatch();
                }
                ConsumerRecords<Long, EnrichedStatus> stRecords = statusConsumer.poll(100);
                if (stRecords.count() != 0) {
                    for (ConsumerRecord<Long, EnrichedStatus> stRecord : stRecords) {
                        if (stRecord.value() != null) {
                            EnrichedStatus enrStatus = stRecord.value();
                            psStatus.setInt(1, enrStatus.getCar_id());
                            psStatus.setInt(2, enrStatus.getTime());
                            psStatus.setString(3, enrStatus.getCar_reg());
                            psStatus.setString(4, enrStatus.getMsg_id());
                            psStatus.setString(5, enrStatus.getCar_status());
                            psStatus.addBatch();
                            
                            if (++count % batchSize == 0)
                                psStatus.executeBatch();
                        }
                    }
                    psStatus.executeBatch();
                }   
            } 
        } catch (SQLException sqlEx) {
            System.out.println("SQLException: "+ sqlEx.getMessage());
            System.out.println("SQLState: "+ sqlEx.getSQLState());
            System.out.println("VendorError: "+ sqlEx.getErrorCode());
        } finally {
            dbConnection.close(psStatus);
            dbConnection.close(psSpeed);
            dbConnection.close(psHeartBeat);
            statusConsumer.close();
            speedConsumer.close();
            heartbeatConsumer.close();
            dbConnection.destroy();
        }
    }
    
    public static void main(String[] args) {
        ReportEntity re = new ReportEntity();
        re.runReportEntity();
    }
}
