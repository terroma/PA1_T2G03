package as.pa1.serialization;

import as.pa1.data.objets.Status;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 *
 * @author Bruno Assunção 89010
 * @author Hugo Chaves  90842
 * 
 */

public class StatusSerializer implements Serializer<Status> {
    
    private final String encoding = "UTF-8";

    @Override
    public void configure(Map<String, ?> map, boolean bln) {    }

    @Override
    public byte[] serialize(String topic, Status status) {
        int sizeOfMsg_id;
        int sizeOfCar_status;
        byte[] serializedMsg_id;
        byte[] serializedCar_status;
        
        try {
            if (status == null) {
                System.out.println("Null recieved at StatusSerializer.");
                return null;
            } else {
                serializedMsg_id = status.getMsg_id().getBytes(encoding);
                sizeOfMsg_id = serializedMsg_id.length;
                serializedCar_status = status.getCar_status().getBytes(encoding);
                sizeOfCar_status = serializedCar_status.length;

                ByteBuffer buf = ByteBuffer.allocate(4+4+4+sizeOfMsg_id+4+sizeOfCar_status);
                buf.putInt(status.getCar_id());
                buf.putInt(status.getTime());
                buf.putInt(sizeOfMsg_id);
                buf.put(serializedMsg_id);
                buf.putInt(sizeOfCar_status);
                buf.put(serializedCar_status);
                
                return buf.array();
            }
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error serializing Status to byte[].");
        }
    }

    @Override
    public void close() {   }

}
