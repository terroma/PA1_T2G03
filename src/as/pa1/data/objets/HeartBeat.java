package as.pa1.data.objets;

public class HeartBeat {
    private int car_id;
    private int time;
    private String msg_id;
    
    public HeartBeat (int car_id, int time, String msg_id) {
        this.car_id = car_id;
        this.time = time;
        this.msg_id = msg_id;
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
    
    @Override
    public String toString() {
        return String.join("|",
                String.format("%02d", car_id),
                Integer.toString(time),
                msg_id);
    }
}
