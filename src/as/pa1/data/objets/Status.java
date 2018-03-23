package as.pa1.data.objets;

public class Status {
    private int car_id;
    private int time;
    private String msg_id;
    private String car_status;
    
    public Status (int car_id, int time, String msg_id, String car_status) {
        this.car_id = car_id;
        this.time = time;
        this.msg_id = msg_id;
        this.car_status = car_status;
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
     * @return the car_status
     */
    public String getCar_status() {
        return car_status;
    }
}
