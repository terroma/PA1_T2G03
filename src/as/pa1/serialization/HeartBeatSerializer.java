package as.pa1.serialization;

import as.pa1.data.objets.HeartBeat;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class HeartBeatSerializer implements Serializer<HeartBeat> {
    
    private final String encoding = "UTF-8";
    
    @Override
    public void configure(Map<String, ?> map, boolean bln) {    }

    @Override
    public byte[] serialize(String topic, HeartBeat heartbeat) {
        int sizeOfMsg_id;
        byte[] serializedMsg_id;
        
        try {
            if (heartbeat == null) {
                System.out.println("Null recieved in HeartBeatSerializer.");
                return null;
            } else {
                serializedMsg_id = heartbeat.getMsg_id().getBytes(encoding);
                sizeOfMsg_id = serializedMsg_id.length;
                
                ByteBuffer buf = ByteBuffer.allocate(4+4+4+sizeOfMsg_id);
                buf.putInt(heartbeat.getCar_id());
                buf.putInt(heartbeat.getTime());
                buf.putInt(sizeOfMsg_id);
                buf.put(serializedMsg_id);
                
                return buf.array();
            }            
        } catch (Exception e) {
            throw new SerializationException("Error serializing HeartBeat to byte[].");
        }
    }

    @Override
    public void close() {   }
    
}
