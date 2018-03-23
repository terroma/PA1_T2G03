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
    public void configure(Map<String, ?> map, boolean bln) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public byte[] serialize(String topic, HeartBeat data) {
        int sizeOfCar_id;
        int sizeOfTime;
        int sizeOfMsg_id;
        byte[] serializedCar_id;
        byte[] serializedTime;
        byte[] serializedMsg_id;
        
        try {
            if (data != null) {
                return null;
            } else {
                serializedCar_id = Integer.toString(data.getCar_id()).getBytes(encoding);
                sizeOfCar_id = serializedCar_id.length;
                serializedTime = Integer.toString(data.getTime()).getBytes(encoding);
                sizeOfTime = serializedTime.length;
                serializedMsg_id = data.getMsg_id().getBytes(encoding);
                sizeOfMsg_id = serializedMsg_id.length;
                
                ByteBuffer buf = ByteBuffer.allocate(4+4+sizeOfCar_id+4+sizeOfTime+4+sizeOfMsg_id);
                buf.putInt(sizeOfCar_id);
                buf.put(serializedCar_id);
                buf.putInt(sizeOfTime);
                buf.put(serializedTime);
                buf.putInt(sizeOfMsg_id);
                buf.put(serializedMsg_id);
                
                return buf.array();
            }
            
            
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serializing HB to byte[]");
        }
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
