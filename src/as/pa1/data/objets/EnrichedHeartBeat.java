package as.pa1.data.objets;

public class EnrichedHeartBeat extends HeartBeat {
    private String car_reg;
    
    public EnrichedHeartBeat(int car_id, int time, String car_reg, String msg_id) {
        super(car_id, time, msg_id);
        this.car_reg = car_reg;
    }

    /**
     * @return the car_reg
     */
    public String getCar_reg() {
        return car_reg;
    }
    
}
