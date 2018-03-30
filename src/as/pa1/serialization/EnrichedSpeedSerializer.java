package as.pa1.serialization;

import as.pa1.data.objets.EnrichedSpeed;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class EnrichedSpeedSerializer implements Serializer<EnrichedSpeed> {
    
    private final String encoding = "UTF-8";

    @Override
    public void configure(Map<String, ?> map, boolean bln) {    }

    @Override
    public byte[] serialize(String topic, EnrichedSpeed enrichedSPEED) {
        int sizeOfCar_reg;
        int sizeOfMsg_id;
        byte[] serializedCar_reg;
        byte[] serializedMsg_id;
        
        try {
            if (enrichedSPEED == null) {
                System.out.println("Null recieved in EnrichedSpeedSerializer.");
                return null;
            } else {
                serializedCar_reg = enrichedSPEED.getCar_reg().getBytes(encoding);
                sizeOfCar_reg = serializedCar_reg.length;
                serializedMsg_id = enrichedSPEED.getMsg_id().getBytes(encoding);
                sizeOfMsg_id = serializedMsg_id.length;
                
                ByteBuffer buf = ByteBuffer.allocate(4+4+4+sizeOfCar_reg+4+sizeOfMsg_id+4+4+4);
                buf.putInt(enrichedSPEED.getCar_id());
                buf.putInt(enrichedSPEED.getTime());
                buf.putInt(sizeOfCar_reg);
                buf.put(serializedCar_reg);
                buf.putInt(sizeOfMsg_id);
                buf.put(serializedMsg_id);
                buf.putInt(enrichedSPEED.getSpeed());
                buf.putInt(enrichedSPEED.getLocalization());
                buf.putInt(enrichedSPEED.getMax_speed());
                
                return buf.array();
            }
        } catch (Exception e) {
            throw new SerializationException("Error when serializing EnrichedSpeed to byte[].");
        }
    }

    @Override
    public void close() {   }
    
}
