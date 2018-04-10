/**
 *
 * @author Bruno Assunção 89010
 * @author Hugo Chaves  90842
 * 
 */
package as.pa1.alarmentity;

public enum Alarm {
    ON("on"), OFF("off");
    
    private final String value;
    
    private Alarm(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
    
}
