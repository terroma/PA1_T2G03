package as.pa1.serialization;

import as.pa1.data.objets.HeartBeat;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class HeartBeatDeserializer implements Deserializer<HeartBeat> {
    private final String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> map, boolean bln) {    }

    @Override
    public HeartBeat deserialize(String topic, byte[] data) {
        
        try {
            if (data == null) {
                System.out.println("Null recieved at HeartBeatDeserializer.");
                return null;
            } else {
                ByteBuffer buf = ByteBuffer.wrap(data);
                int car_id = buf.getInt();
                int time = buf.getInt();
                
                int sizeOfMsg_id = buf.getInt();
                byte[] msg_idBytes = new byte[sizeOfMsg_id];
                buf.get(msg_idBytes);
                String msg_id = new String(msg_idBytes,encoding);
                
                return new HeartBeat(car_id,time,msg_id);
            }
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to HeartBeat.");
        }
    }

    @Override
    public void close() {   }
    
}
