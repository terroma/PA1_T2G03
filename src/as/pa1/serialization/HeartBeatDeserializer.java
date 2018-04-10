package as.pa1.serialization;

import as.pa1.data.objets.HeartBeat;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 *
 * @author Bruno Assunção 89010
 * @author Hugo Chaves  90842
 * 
 */

public class HeartBeatDeserializer implements Deserializer<HeartBeat> {
    
    private final String encoding = "UTF-8";

    @Override
    public void configure(Map<String, ?> map, boolean bln) {    }

    @Override
    public HeartBeat deserialize(String topic, byte[] heartbeat) {
        
        try {
            if (heartbeat == null) {
                System.out.println("Null recieved at HeartBeatDeserializer.");
                return null;
            } else {
                ByteBuffer buf = ByteBuffer.wrap(heartbeat);
                int deserializedCar_id = buf.getInt();
                int deserializedTime = buf.getInt();
                
                int sizeOfMsg_id = buf.getInt();
                byte[] msg_idBytes = new byte[sizeOfMsg_id];
                buf.get(msg_idBytes);
                String deserializedMsg_id = new String(msg_idBytes,encoding);
                
                return new HeartBeat(deserializedCar_id,deserializedTime,deserializedMsg_id);
            }
        } catch (Exception e) {
            throw new SerializationException("Error deserializing byte[] to HeartBeat.");
        }
    }

    @Override
    public void close() {   }
    
}
