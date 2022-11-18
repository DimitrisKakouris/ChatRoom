package distributed.publisher;

import distributed.datastructure.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;

public class PubToBrokerConnection extends Thread {
	private Socket socket;
	private final ArrayList<Topic> topics;
	ArrayList<Topic> interestedΤopics = new ArrayList<>();
	ObjectOutputStream out = null;
	ObjectInputStream in = null;
	Object obj;

	private final ReaderForPublisher message_db;

	PubToBrokerConnection(Socket socket, ArrayList<Topic> topics, ReaderForPublisher message_db) {
		this.socket = socket;
		this.topics = topics;
		this.message_db = message_db;
	}

	public synchronized void run() {
		System.out.println("[+] Connection with " + IPtools.getRemoteIP(socket) + ":" + socket.getPort() + " started!");
		try {
			out = new ObjectOutputStream(socket.getOutputStream());
			in = new ObjectInputStream(socket.getInputStream());


			out.writeUTF("PUBLISHER");
			out.flush();

			//Send the topics that this Publisher is responsible for
			out.writeObject(topics);
			out.flush();

			//Send the list of the other brokers
			out.writeObject(Publisher.brokers);
			out.flush();


			while (true) try {
				obj = in.readObject();
				interestedΤopics = (ArrayList<Topic>) obj;
				break;
			} catch (ClassCastException e) {
				out.writeUTF("Error - Invalid object received, waiting again for ArrayList<Topic>.");
				out.flush();
			} catch (ClassNotFoundException e) {
				out.writeUTF("Error - No object received, waiting again for ArrayList<Topic>.");
				out.flush();
			}
			for (Topic topic : interestedΤopics)
				System.out.println(IPtools.getRemoteIP(socket) + ":" + socket.getPort() +
						" is interested in: " + topic.getGroupName());
			//Now the topics that this broker is interested in are in the interestedTopics ArrayList


			Object obj;
			while (true) { //Listen for UPDATE commands
				String msg = in.readUTF();
				if (msg.equals("UPDATE_INTERESTED_TOPICS")) {
					//Sub want's to update the list that he is interested in
					ArrayList<Topic> intTopics = (ArrayList<Topic>) in.readUnshared();
					interestedΤopics = intTopics;

					System.out.print("\n"+IPtools.getRemoteIP(socket) + ":" + socket.getPort() + " is now interested in: (");
					for(Topic topic : intTopics)
						System.out.print(" " + topic.getGroupName());
					System.out.print(")\n");
				} else if(msg.equals("MESSAGE")) {
					Data data = (Data) in.readUnshared();
					message_db.messageReceived(data);
				}
			}
		} catch (SocketException e) {
		} catch (EOFException e) {
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				brokerConclusion();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

	}

	void writeObj(Object o) throws IOException {
		if(out != null) {
			System.out.println("Sending data to broker");
			out.writeObject(o);
			out.flush();
		}
	}

	synchronized void brokerConclusion() throws UnknownHostException {
		System.out.println("Broker " + IPtools.getRemoteIP(socket) + ":" + socket.getPort() + " disconnected.");
		Iterator<BrokerObj> it = Publisher.brokers.iterator();

		int i = 0;
		while (it.hasNext()) {
			BrokerObj broker = it.next();

			//If it is the BrokerObj who disconnected remove him
			if (broker.getIp().equals(IPtools.getRemoteIP(socket)) && broker.getPort() == socket.getPort()) {
				it.remove();
				Publisher.connections.remove(i);
				continue;
			}
			//He isn't the broker which disconnected so we have to tell him that one broker is disconnected
			PubToBrokerConnection connection = Publisher.connections.get(i);
			try {
				//Send the special message
				connection.writeObj(new Data(null, new Topic("0"), new Value("DISCONNECTED " + IPtools.getRemoteIP(socket) + " " + socket.getPort()), ""));
			} catch (IOException e) {
				e.printStackTrace();
			}
			i++;
		}
	}
}



