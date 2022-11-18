package distributed.datastructure;

import java.io.Serializable;

public class Value implements Serializable {
    
    private String specialMessage;

    public Value(String specialMessage) {
    	this.specialMessage = specialMessage;
	}

	public String getSpecialMessage() { return specialMessage; }

	@Override
    public String toString() {
    	if(specialMessage != null)
    		return specialMessage;
        return "Value: ";
    }
}
