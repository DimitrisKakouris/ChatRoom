package distributed.subscriber;
import distributed.datastructure.*;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;


public class SubToBrokerConnection extends Thread {
	Socket socket;
	ObjectOutputStream out = null;
	ObjectInputStream in = null;
	ArrayList<Topic> interestedTopics = new ArrayList<>();
	boolean initialized = false;

	SubToBrokerConnection(Socket socket) throws IOException {
		this.socket = socket;
		out = new ObjectOutputStream(socket.getOutputStream());
		in = new ObjectInputStream(socket.getInputStream());
	}
	SubToBrokerConnection(Socket socket, ArrayList<Topic> interestedTopics) throws IOException {
		this.socket = socket;
		out = new ObjectOutputStream(socket.getOutputStream());
		in = new ObjectInputStream(socket.getInputStream());
		this.interestedTopics = interestedTopics;
	}

	public synchronized	 void run() {
		try {

			out.writeUTF("SUBSCRIBER");
			out.flush();

			Subscriber.brokers = (ArrayList<BrokerObj>) in.readObject();

			Subscriber.topicsBroker = (HashMap<Topic, BrokerObj>) in.readObject();
			//Send the topics that we are interested in(empty list as for now)
			out.writeObject(interestedTopics);
			out.flush();

			initialized = true;

			while(true) {
				try{
				Subscriber.received((Data) in.readUnshared());}
				catch (SocketException e){
					System.out.println("[-] Broker down. Requesting from another Broker!");
					break;
				} catch (EOFException e){
					System.out.println("[-] Broker down. Requesting from another Broker!");
					break;
				} catch (ClassCastException e) {
					System.out.println("Topic over. Moving on.");
					break;
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			//Get the topics that this broker was responsible for
			ArrayList<Topic> lostTopics = new ArrayList<>();
			ArrayList<Topic> lostInterestedTopics = new ArrayList<>();
			BrokerObj thisBroker = new BrokerObj(IPtools.getRemoteIP(socket), socket.getPort());


			Iterator it = Subscriber.topicsBroker.entrySet().iterator();
			while (it.hasNext()) {
				HashMap.Entry pair = (HashMap.Entry)it.next();
				if(pair.getValue().equals(thisBroker)) { //We have to remove & save them
					Topic t = (Topic) pair.getKey();
					lostTopics.add(t);
					if(isInterestedIn(t))
						lostInterestedTopics.add(t);
					it.remove();
				}
			}

			System.out.print("\nBroker " + thisBroker.getIp() + ":" + thisBroker.getPort() +
							" disconnected so we have to connect to another broker for these topics: ");
			for(Topic topic : lostTopics)
				System.out.print(" " + topic.getGroupName());
			System.out.println();


			ArrayList<BrokerObj> brokers = Subscriber.brokers;

			Subscriber.removeBroker(thisBroker);

			System.out.println("topicprin1");
			addTopicsToBrokerHashMap(brokers, lostTopics, Subscriber.topicsBroker);
			System.out.println("topicprin2");
			for(Topic topic : lostInterestedTopics) {
				try {
					System.out.println("topic");
					Subscriber.addTopicToInterestedList(topic);

				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}

	private boolean isInterestedIn(Topic topic) {
		for(Topic t : this.interestedTopics)
			if(t.equals(topic))
				return true;
		return false;
	}

	void updateList(ArrayList<Topic> topics) throws IOException {
		out.writeUTF("UPDATE_INTERESTED_TOPICS");
		out.flush();

		interestedTopics = topics;
		out.writeUnshared(topics);
		out.flush();
	}

	void sendMessage(Data msg) throws IOException {
		out.writeUTF("MESSAGE");
		out.flush();
		out.writeUnshared(msg);
		out.flush();
	}

	public static void addTopicsToBrokerHashMap(ArrayList<BrokerObj> brokers, ArrayList<Topic> topicsForInsertion, HashMap<Topic, BrokerObj> topicsBroker) {
		//This method adds Topics to the HashMap
		String hash;
		if(brokers.isEmpty())
			System.out.println("No brokers!");
		for (Topic topic : topicsForInsertion) {
			hash = md5(topic.getGroupName());

			//Find the appropriate broker
			if(brokers.get(0).getHash().compareTo(hash) >= 0)// topic's hash <= broker1's hash
				topicsBroker.put(topic, brokers.get(0));

			else if(brokers.get(brokers.size() - 1).getHash().compareTo(hash) <= 0) // topic's hash >= last_broker's hash
				topicsBroker.put(topic, brokers.get(brokers.size() - 1));

			else {
				for (int i = 0; i < brokers.size() - 1; i++) {

					//Check if topic's hash is between brokers[i] and brokers[i+1]
					if (brokers.get(i).getHash().compareTo(hash) <= 0 && brokers.get(i + 1).getHash().compareTo(hash) >= 0) {
						topicsBroker.put(topic, brokers.get(i));
						break;
					}

				}
			}
		}
	}



	public static String md5(String input) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] messageDigest = md.digest(input.getBytes());
			BigInteger no = new BigInteger(1, messageDigest);
			String hash = no.toString(16);
			while (hash.length() < 32) {
				hash = "0" + hash;
			}
			return hash;
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
}
