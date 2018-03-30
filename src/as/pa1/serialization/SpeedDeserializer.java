package as.pa1.serialization;

import as.pa1.data.objets.Speed;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class SpeedDeserializer implements Deserializer<Speed> {
    
    private final String encoding = "UTF-8";

    @Override
    public void configure(Map<String, ?> map, boolean bln) {    }

    @Override
    public Speed deserialize(String topic, byte[] speed) {
        try {
            if (speed == null) {
                System.out.println("Null recieved in SpeedDeserializer.");
                return null;
            } else {
                ByteBuffer buf = ByteBuffer.wrap(speed);
                int deserializedCar_id = buf.getInt();
                int deserializedTime = buf.getInt();
                
                int sizeOfMsg_id = buf.getInt();
                byte[] msg_idBytes = new byte[sizeOfMsg_id];
                buf.get(msg_idBytes);
                String deserializedMsg_id = new String(msg_idBytes,encoding);
                
                int deserializedSpeed = buf.getInt();
                int deserializedLocalization = buf.getInt();
                
                return new Speed(deserializedCar_id,deserializedTime,deserializedMsg_id,deserializedSpeed,deserializedLocalization);
            }
        } catch (Exception e) {
            throw new SerializationException("Error deserializing byte[] to Speed.");
        }
    }

    @Override
    public void close() {   }
    
}
