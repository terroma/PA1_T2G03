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
    public byte[] serialize(String topic, HeartBeat data) {
        int sizeOfMsg_id;
        byte[] serializedMsg_id;
        
        try {
            if (data == null) {
                System.out.println("Null recieved in HeartBeatSerializer!");
                return null;
            } else {
                serializedMsg_id = data.getMsg_id().getBytes(encoding);
                sizeOfMsg_id = serializedMsg_id.length;
                
                ByteBuffer buf = ByteBuffer.allocate(4+4+4+sizeOfMsg_id);
                buf.putInt(data.getCar_id());
                buf.putInt(data.getTime());
                buf.putInt(sizeOfMsg_id);
                buf.put(serializedMsg_id);
                
                return buf.array();
            }            
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serializing HeartBeat to byte[]!");
        }
    }

    @Override
    public void close() {   }
    
}
