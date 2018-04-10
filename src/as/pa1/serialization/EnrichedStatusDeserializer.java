package as.pa1.serialization;

import as.pa1.data.objets.EnrichedStatus;
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

public class EnrichedStatusDeserializer implements Deserializer<EnrichedStatus> {
    
    private final String encoding = "UTF-8";

    @Override
    public void configure(Map<String, ?> map, boolean bln) {    }

    @Override
    public EnrichedStatus deserialize(String topic, byte[] enrichedSTATUS) {
        try {
            if (enrichedSTATUS == null) {
                System.out.println("Null recieved in EnrichedStatusDeserializer.");
                return null;
            } else {
                ByteBuffer buf = ByteBuffer.wrap(enrichedSTATUS);
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
                
                int sizeOfCar_status = buf.getInt();
                byte[] car_statusBytes = new byte[sizeOfCar_status];
                buf.get(car_statusBytes);
                String deserializedCar_status = new String(car_statusBytes,encoding);
                
                return new EnrichedStatus(deserializedCar_id, deserializedTime, deserializedCar_reg, deserializedMsg_id, deserializedCar_status);
            }
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to EnrichedStatus.");
        }
    }

    @Override
    public void close() {   }
    
}
