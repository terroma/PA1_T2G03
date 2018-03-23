package as.pa1.data.objets;

public class Speed {
    private int car_id;
    private int time;
    private String msg_id;
    private int speed;
    private int localization;
    
    public Speed (int car_id, int time, String msg_id, int speed, int localization) {
        this.car_id = car_id;
        this.time = time;
        this.msg_id = msg_id;
        this.speed = speed;
        this.localization = localization;
    }

    /**
     * @return the car_id
     */
    public int getCar_id() {
        return car_id;
    }

    /**
     * @return the time
     */
    public int getTime() {
        return time;
    }

    /**
     * @return the msg_id
     */
    public String getMsg_id() {
        return msg_id;
    }

    /**
     * @return the speed
     */
    public int getSpeed() {
        return speed;
    }

    /**
     * @return the localization
     */
    public int getLocalization() {
        return localization;
    }
}
