package distributed.broker;

import distributed.datastructure.Data;
import distributed.datastructure.Topic;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class Subscriber {
	private String ip;
	private int port;
	private ArrayList<Topic> interestedTopics;

	private ObjectOutputStream out;

	Subscriber(String ip, int port, ArrayList<Topic> interestedTopics, ObjectOutputStream out) {
		this.ip = ip;
		this.port = port;
		this.interestedTopics = interestedTopics;
		this.out = out;
	}



	public ArrayList<Topic> getInterestedTopics() {
		return interestedTopics;
	}

	public void setInterestedTopics(ArrayList<Topic> interestedTopics) {
		this.interestedTopics = interestedTopics;
	}

	public void messageReceived(Data msg) {

		try {
			BrokerServerSocket.connectionToPublisher.writeUTF("MESSAGE");
			BrokerServerSocket.connectionToPublisher.flush();

			BrokerServerSocket.connectionToPublisher.writeUnshared(msg);
			BrokerServerSocket.connectionToPublisher.flush();
		} catch(IOException e) {
			e.printStackTrace();
		}

	}
}
