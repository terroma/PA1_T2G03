package as.pa1.serialization;

import as.pa1.data.objets.EnrichedHeartBeat;
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

public class EnrichedHeartBeatSerializer implements Serializer<EnrichedHeartBeat> {

    private final String encoding = "UTF-8";
    
    @Override
    public void configure(Map<String, ?> map, boolean bln) {    }

    @Override
    public byte[] serialize(String topic, EnrichedHeartBeat enrichedHB) {
        int sizeOfCar_reg;
        int sizeOfMsg_id;
        byte[] serializedCar_reg;
        byte[] serializedMsg_id;
        
        try {
            if (enrichedHB == null) {
                System.out.println("Null recieved in EnrichedHeartBeatSerializer.");
                return null;
            } else {
                serializedCar_reg = enrichedHB.getCar_reg().getBytes(encoding);
                sizeOfCar_reg = serializedCar_reg.length;
                serializedMsg_id = enrichedHB.getMsg_id().getBytes(encoding);
                sizeOfMsg_id = serializedMsg_id.length;
                
                ByteBuffer buf = ByteBuffer.allocate(4+4+4+sizeOfCar_reg+4+sizeOfMsg_id);
                buf.putInt(enrichedHB.getCar_id());
                buf.putInt(enrichedHB.getTime());
                buf.putInt(sizeOfCar_reg);
                buf.put(serializedCar_reg);
                buf.putInt(sizeOfMsg_id);
                buf.put(serializedMsg_id);
                
                return buf.array();
            }
        } catch (Exception e) {
            throw new SerializationException("Error when serializing EnrichedHeartBeat to byte[].");
        }
    }

    @Override
    public void close() {   }
    
}
