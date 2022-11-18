package distributed.subscriber;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;

import distributed.datastructure.*;
import java.util.*;

public class Subscriber {
	public static ArrayList<BrokerObj> brokers = new ArrayList<>();

	static Socket socket;

	static HashMap<BrokerObj, SubToBrokerConnection> brokerConnections = new HashMap<>();
	static HashMap<Topic, BrokerObj> topicsBroker = new HashMap<>();

    public static void main(String[] args) throws IOException, InterruptedException {
    	//!--Please provide arguments "resources/sub.txt resources/brok.txt"
		//Doesn't matter it says ForPublisher...
		//We're just borrowing the txt parse methods from this class
		ReaderForPublisher reader;
		if(args.length == 0) {
			System.out.println("No args! : getting topics from pub.txt and brokers from brok.txt!!!");
			reader = new ReaderForPublisher(null,null);
		}else {
			reader = new ReaderForPublisher(args[0],args[1]);
		}

		//firstbroker gets ip and port from the first element in brok.txt file or whatever txt passed as a second arg
		BrokerObj firstBroker = new BrokerObj(reader.getBrokerObjs().get(0).getIp(),reader.getBrokerObjs().get(0).getPort());
		System.out.println("Starting connection with initial broker: " + reader.getBrokerObjs().get(0).getIp() + ":" +reader.getBrokerObjs().get(0).getPort());

		try {
			socket = new Socket((firstBroker.getIp()), firstBroker.getPort());
			SubToBrokerConnection con = new SubToBrokerConnection(socket);
			con.start();
			brokerConnections.put(firstBroker, con);

		} catch (UnknownHostException unknownHost) {
			System.err.println("You are trying to connect to an unknown host!");
		} catch (IOException ioException) {
			if (ioException.getMessage().contains("Connection refused")){
				System.out.println("Connection refused. Check IP again.");
				System.exit(1);
			}
			ioException.printStackTrace();
		}

		Thread.sleep(2000);




		//This list has the topics the sub is interested in, provided by the file in the first arg -> resources/sub.txt
		ArrayList<Topic> topicsTheSubIsInterested = reader.getTopicsInterested();
		Thread.sleep(2000);
		for(Topic topic : topicsTheSubIsInterested){
			Thread.sleep(1000);
			addTopicToInterestedList(topic);
		}


    }

	public static void received(Data data) {
		String includesFile = "";
		if(data.hasFileAttached())
			includesFile = "INCLUDES FILE.";

		System.out.print(data.getUsername() + "@" + data.getKey().getGroupName() + ": " + data.getValue() + ". " + includesFile + "\n-> ");

	}

	static synchronized void addTopicToInterestedList(Topic topic) throws IOException, InterruptedException {
    	BrokerObj broker = null;


    	//Iterate HashMap
		for (HashMap.Entry<Topic, BrokerObj> element : topicsBroker.entrySet()) {
			broker = element.getValue();
			break;

		}

		appendToInterestedList(broker, topic);
	}

	static synchronized void appendToInterestedList(BrokerObj broker, Topic topic) throws InterruptedException, IOException {
		if(broker == null) {
			System.out.println("[-] Invalid key!");
			return;
		}
		System.out.println("[SYSTEM] Trying to connect to " + broker.getIp() + ":" + broker.getPort() + " for topic: " + topic.getGroupName());
		SubToBrokerConnection connection = null;
		for (HashMap.Entry<BrokerObj, SubToBrokerConnection> element : brokerConnections.entrySet()) {

			if(broker.equals(element.getKey())) {
				connection = element.getValue();
				break;
			}
		}

		if(connection == null) {
			//No open connection with that broker, let's open one
			try {

				Socket s = new Socket((broker.getIp()), broker.getPort());
				ArrayList<Topic> list = new ArrayList<>();
				list.add(topic);
				SubToBrokerConnection con = new SubToBrokerConnection(s, list);
				con.start();
				brokerConnections.put(broker, con);
			} catch (UnknownHostException unknownHost) {
				System.err.println("Host error.");
			} catch (IOException ioException) {
				ioException.printStackTrace();
			} finally {
				return; //Either the connection failed or we made a new connection with interestedList containing the topic
				//In both case there is no need to continue the execution
			}
		}

		while(!connection.initialized)
			Thread.sleep(500); //Wait for the connection to be initialized

		ArrayList<Topic> newList = connection.interestedTopics;
		newList.add(topic);
		connection.updateList(newList);
		Scanner sc= new Scanner(System.in);
		System.out.print("Username -> ");
		String username = sc.next();
		String input, filepath;

		while(true) {
			System.out.print("-> ");
			input= sc.nextLine();

			List<String> data_input = Arrays.asList(input.split(","));

			if (data_input.size() < 2)
				System.out.println("Format: groupname,message(,filepath)?");
			else {
				filepath = "";
				if(data_input.size() >=3)
					filepath = data_input.get(2);
				connection.sendMessage(new Data(username, new Topic(data_input.get(0)), new Value(data_input.get(1)), filepath));
			}
		}
	}

	public static void removeBroker(BrokerObj thisBroker) {
		for(int i = 0; i < brokers.size(); i++) {
			if(brokers.get(i).equals(thisBroker)) {
				brokers.remove(i);

				break;
			}
			i++;
		}
	}
}