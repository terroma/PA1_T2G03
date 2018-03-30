package as.pa1.serialization;

import as.pa1.data.objets.EnrichedSpeed;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class EnrichedSpeedDeserializer implements Deserializer<EnrichedSpeed> {
    
    private final String encoding = "UTF-8";

    @Override
    public void configure(Map<String, ?> map, boolean bln) {    }

    @Override
    public EnrichedSpeed deserialize(String topic, byte[] enrichedSPEED) {
        try {
            if (enrichedSPEED == null) {
                System.out.println("Null recieved in EnrichedSpeedDeserializer.");
                return null;
            } else {
                ByteBuffer buf = ByteBuffer.wrap(enrichedSPEED);
                int deserializedCar_id = buf.getInt();
                int deserializedTime = buf.getInt();
                
                int sizeOfCar_reg = buf.getInt();
                byte[] car_regBytes = new byte[sizeOfCar_reg];
                buf.get(car_regBytes);
                String deserializedCar_reg = new String(car_regBytes,encoding);
               
                int sizeOfMsg_id = buf.getInt();
                byte[] msg_idBytes = new byte[sizeOfMsg_id];
                buf.get(msg_idBytes);
                String deserializedMsg_id = new String(msg_idBytes,encoding);
                
                int deserializedSpeed = buf.getInt();
                int deserializedLocalization = buf.getInt();
                int deserializedMax_speed = buf.getInt();
                
                return new EnrichedSpeed(deserializedCar_id, deserializedTime, deserializedCar_reg, deserializedMsg_id, deserializedSpeed, deserializedLocalization, deserializedMax_speed);
            }
        } catch (Exception e) {
            throw new SerializationException("Error when deserealizing byte[] to EnrichedSpeed.");
        }
    }

    @Override
    public void close() {   }
    
}
