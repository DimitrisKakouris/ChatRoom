package distributed.broker;

import distributed.datastructure.Data;
import distributed.datastructure.IPtools;
import distributed.datastructure.Topic;
import distributed.datastructure.Value;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class BrokerSubConnection extends Thread {
	ObjectOutputStream out;
	ObjectInputStream in;

	Socket socket;
	Subscriber sub = null;
	BrokerSubConnection(Socket socket, ObjectOutputStream out, ObjectInputStream in) {
		this.socket = socket;
		this.out= out;
		this.in = in;
	}

	public synchronized void run() {
		try {
			out.writeObject(Broker.brokers); //Send the broker list
			out.flush();

			out.writeObject(Broker.topicsBroker); //Send <Topic, BrokerObj> HashMap to the Subscriber
			out.flush();

			//Now consumer sends an ArrayList<Topic> containing the Topics he is interested in.
			ArrayList<Topic> interestedTopics;

			interestedTopics = (ArrayList<Topic>) in.readObject();

			sub = new Subscriber(IPtools.getRemoteIP(socket), socket.getPort(), interestedTopics, out);

			Broker.subscribers.add(sub);
			Broker.openConnections.put(sub, this);

			while(true) { //Listen for UPDATE commands

				String msg = in.readUTF();
				if(msg.equals("UPDATE_INTERESTED_TOPICS")) {
					System.out.println(IPtools.getRemoteIP(socket) + ":" + socket.getPort() + " updated his interested list!");
					//Sub want's to update the list that he is interested in
					ArrayList<Topic> intTopics = (ArrayList<Topic>) in.readUnshared();
					sub.setInterestedTopics(intTopics);
				} else if(msg.equals("MESSAGE")) {

					Data msg_received = (Data) in.readUnshared();
					sub.messageReceived(msg_received);
				}
			}


		} catch (IOException e) {
//			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			System.out.println("Connection closed.");

			try {
				in.close();
				out.close();
				socket.close();
				if(sub != null) {
					Broker.subscribers.remove(sub);
					Broker.openConnections.remove(sub);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void sendData(Data data) {
		sendData send = new sendData(out, data);
		send.start();
	}
}

class sendData extends Thread {

	private final Data data;
	private final ObjectOutputStream out;

	public sendData(ObjectOutputStream out, Data data) {
		this.out = out;
		this.data = data;
	}

	public synchronized void run() {
		//Send Data(Topic, value) to the Sub
		try {
			out.writeUnshared(data);
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}