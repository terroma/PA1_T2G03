package as.pa1.serialization;

import as.pa1.data.objets.Speed;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class SpeedSerializer implements Serializer<Speed> {
    
    private final String encoding = "UTF-8";

    @Override
    public void configure(Map<String, ?> map, boolean bln) {    }

    @Override
    public byte[] serialize(String topic, Speed speed) {
        int sizeOfMsg_id;
        byte[] serializedMsg_id;
        
        try {
            if (speed == null) {
                System.out.println("Null recieved in SpeedSerializer.");
                return null;
            } else {
                
                serializedMsg_id = speed.getMsg_id().getBytes(encoding);
                sizeOfMsg_id = serializedMsg_id.length;
                
                ByteBuffer buf = ByteBuffer.allocate(4+4+4+sizeOfMsg_id+4+4);
                buf.putInt(speed.getCar_id());
                buf.putInt(speed.getTime());
                buf.putInt(sizeOfMsg_id);
                buf.put(serializedMsg_id);
                buf.putInt(speed.getSpeed());
                buf.putInt(speed.getLocalization());
                
                return buf.array();
            }   
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error serializing Speed to byte[].");
        }
    }

    @Override
    public void close() {   }

}
