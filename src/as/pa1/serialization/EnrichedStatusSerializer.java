package as.pa1.serialization;

import as.pa1.data.objets.EnrichedStatus;
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

public class EnrichedStatusSerializer implements Serializer<EnrichedStatus> {
    
    private final String encoding = "UTF-8";

    @Override
    public void configure(Map<String, ?> map, boolean bln) {    }

    @Override
    public byte[] serialize(String topic, EnrichedStatus enrichedSTATUS) {
        int sizeOfCar_reg;
        int sizeOfMsg_id;
        int sizeOfCar_status;
        byte[] serializedCar_reg;
        byte[] serializedMsg_id;
        byte[] serializedCar_status;
        
        try {
            if (enrichedSTATUS == null) {
                System.out.println("Null recieved in EnrichedStatusSerializer.");
                return null;
            } else {
                serializedCar_reg = enrichedSTATUS.getCar_reg().getBytes(encoding);
                sizeOfCar_reg = serializedCar_reg.length;
                serializedMsg_id = enrichedSTATUS.getMsg_id().getBytes(encoding);
                sizeOfMsg_id = serializedMsg_id.length;
                serializedCar_status = enrichedSTATUS.getCar_status().getBytes(encoding);
                sizeOfCar_status = serializedCar_status.length;
                
                ByteBuffer buf = ByteBuffer.allocate(4+4+4+sizeOfCar_reg+4+sizeOfMsg_id+4+sizeOfCar_status);
                buf.putInt(enrichedSTATUS.getCar_id());
                buf.putInt(enrichedSTATUS.getTime());
                buf.putInt(sizeOfCar_reg);
                buf.put(serializedCar_reg);
                buf.putInt(sizeOfMsg_id);
                buf.put(serializedMsg_id);
                buf.putInt(sizeOfCar_status);
                buf.put(serializedCar_status);
                
                return buf.array();
            }
        } catch (Exception e) {
            throw new SerializationException("Error when serializing EnrichedStatus to byte[].");
        }
    }

    @Override
    public void close() {   }
    
}
