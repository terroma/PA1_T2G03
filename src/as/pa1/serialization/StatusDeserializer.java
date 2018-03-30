package as.pa1.serialization;

import as.pa1.data.objets.Status;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class StatusDeserializer implements Deserializer<Status> {
    
    private final String encoding = "UTF-8";

    @Override
    public void configure(Map<String, ?> map, boolean bln) {    }

    @Override
    public Status deserialize(String topic, byte[] status) {
        try {
            if (status == null) {
                System.out.println("Null recieved in StatusDeserializer.");
                return null;
            } else {
                ByteBuffer buf = ByteBuffer.wrap(status);
                int deserializedCar_id = buf.getInt();
                int deserializedTime = buf.getInt();
                
                int sizeOfMsg_id = buf.getInt();
                byte[] msg_idBytes = new byte[sizeOfMsg_id];
                buf.get(msg_idBytes);
                String deserializedMsg_id = new String(msg_idBytes,encoding);
                
                int sizeOfStatus = buf.getInt();
                byte[] statusBytes = new byte[sizeOfStatus];
                buf.get(statusBytes);
                String deserializedStatus = new String(statusBytes,encoding);
                
                return new Status(deserializedCar_id,deserializedTime,deserializedMsg_id,deserializedStatus);
            }
        } catch (Exception e) {
            throw new SerializationException("Error deserializing byte[] to Status.");
        }
    }

    @Override
    public void close() {   }
    
}
