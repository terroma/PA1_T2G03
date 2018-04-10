package as.pa1.serialization;

import as.pa1.data.objets.EnrichedHeartBeat;
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

public class EnrichedHeartBeatDeserializer implements Deserializer<EnrichedHeartBeat> {
    
    private final String encoding = "UTF-8";

    @Override
    public void configure(Map<String, ?> map, boolean bln) {    }

    @Override
    public EnrichedHeartBeat deserialize(String topic, byte[] enrichedHB) {
        try {
            if (enrichedHB == null) {
                System.out.println("Null recieved in EnrichedHeartBeatDeserializer.");
                return null;
            } else {
                ByteBuffer buf = ByteBuffer.wrap(enrichedHB);
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
                
                return new EnrichedHeartBeat(deserializedCar_id, deserializedTime, deserializedCar_reg, deserializedMsg_id);
            }
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to EnrichedHeartBeat.");
        }
    }

    @Override
    public void close() {   }
    
}
