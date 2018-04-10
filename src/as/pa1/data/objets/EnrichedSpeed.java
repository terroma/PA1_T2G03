package as.pa1.data.objets;

public class EnrichedSpeed extends Speed {
    private String car_reg;
    private int max_speed;
    
    public EnrichedSpeed(int car_id, int time, String car_reg, String msg_id, int speed, int localization, int max_speed) {
        super(car_id, time, msg_id, speed, localization);
        this.car_reg = car_reg;
        this.max_speed = max_speed;
    }

    /**
     * @return the car_reg
     */
    public String getCar_reg() {
        return car_reg;
    }

    /**
     * @return the max_speed
     */
    public int getMax_speed() {
        return max_speed;
    }
    
    @Override
    public String toString() {
        return String.join(" | ",
                String.format("%02d", this.getCar_id()),
                Integer.toString(this.getTime()),
                car_reg,
                this.getMsg_id(),
                Integer.toString(this.getSpeed()),
                Integer.toString(this.getLocalization()),
                Integer.toString(max_speed));
    }
}
