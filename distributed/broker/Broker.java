package distributed.broker;

import distributed.datastructure.BrokerObj;
import distributed.datastructure.IPtools;
import distributed.datastructure.Topic;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class Broker {

	static String ip;
	static int port;


	static ArrayList<Subscriber> subscribers;
	static ArrayList<Publisher> publishers;
	static ArrayList<BrokerObj> brokers;
	static HashMap<Topic, BrokerObj> topicsBroker;
	static HashMap<Topic, Publisher> topicsPub;

	static HashMap<Subscriber, BrokerSubConnection> openConnections; //Every open connection with Subscriber



	public static void main(String[] args) throws IOException {
		if(args.length == 0) {
			System.out.println("No port specified.");
			System.exit(0);
		}


		ip = IPtools.getLocalIp(); //InetAddress.getLocalHost().getHostAddress() was returning 127.0.0.1 in SOME machines
		port = Integer.parseInt(args[0]);
		//<Topic, BrokerObj> hashmap
		topicsBroker = new HashMap<>();

		brokers = new ArrayList<>();
		publishers = new ArrayList<>(); //Publishers will connect to our ServerSocket and tell us their keys
		subscribers = new ArrayList<>();
		topicsPub = new HashMap<>();
		topicsBroker = new HashMap<>();
		openConnections = new HashMap<>();

		//Server socket
		BrokerServerSocket bServer = new BrokerServerSocket(port);
		bServer.start();

	}

	static void removeBroker(BrokerObj b) {
		for(int i = 0; i < brokers.size(); i++)
			if(brokers.get(i).equals(b)) {
				brokers.remove(i);
				break;
			}
	}


	public static void addTopicsToBrokerHashMap(ArrayList<Topic> topicsForInsertion) {
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

	public static void addTopicsToPubHashMap(ArrayList<Topic> topicsForInsertion, Publisher pub) {
		for(Topic topic : topicsForInsertion) {
			topicsPub.put(topic, pub);
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
