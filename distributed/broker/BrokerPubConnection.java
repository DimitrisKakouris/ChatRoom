package distributed.broker;

import distributed.datastructure.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

public class BrokerPubConnection extends Thread {
	ObjectOutputStream out;
	ObjectInputStream in;
	Socket socket;

	BrokerPubConnection(Socket socket, ObjectOutputStream out, ObjectInputStream in) {
		this.socket = socket;
		this.out = out;
		this.in = in;
	}

	public synchronized void run() {
		ArrayList<Topic> topics = null;
		Publisher pub = null;

		//Add Publisher to the list
		pub = new Publisher(IPtools.getRemoteIP(socket), socket.getLocalPort());

		Broker.publishers.add(pub); //Add pub to the list

		try {
			//========================HEADER========================

			//Let's read the ArrayList with the keys
			topics = (ArrayList<Topic>) in.readObject();

			//Let's read the ArrayList with the other brokers
			Broker.brokers = (ArrayList<BrokerObj>) in.readObject();
			Collections.sort(Broker.brokers);

			//Add hash to the brokers

			//We have to add  the topics to (Topic, Publisher) HashMap
			Broker.addTopicsToPubHashMap(topics, pub);

			//We have to add the topics to (Topic, Broker) HashMap
			Broker.addTopicsToBrokerHashMap(topics);

			//We have to tell to Publisher about what topics we are interested in
			//Lets remove the topics that Publisher has but they belong to another Broker

			Iterator it = topics.iterator();
			while (it.hasNext()) {
				BrokerObj responsibleBroker = Broker.topicsBroker.get(it.next());
				if(!responsibleBroker.getIp().equals(Broker.ip) || responsibleBroker.getPort() != Broker.port)
					it.remove();
			}

			//Now we have the topics that we and the Publisher are responsible for in the ArrayList topics so let's send them
			out.writeObject(topics);
			out.flush();

			//========================END OF HEADER========================


			//Waiting for values

			while(true) {
				pull(in.readObject());
			}




		} catch (IOException e) {
//			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally  {
			try {
				//Publisher Disconnected
				in.close();
				out.close();
				socket.close();

				Broker.publishers.remove(pub);
				for(Topic topic : topics) { //For every topic that his publisher was the provider

					//Tell subscriber that there is a problem
					Data data = new Data(null, topic, new Value("Error - Provider disconnected."), ""); //Send special message value
					pull(data); //Send it to the interested subs

					Broker.topicsPub.remove(topic);
					Broker.topicsBroker.remove(topic);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	private void pull(Object o) {
		try {
			Data data = (Data) o; //Data(key, value)

			//Check if there is a broker disconnection special message
			if(data.getValue().getSpecialMessage() != null && data.getValue().getSpecialMessage().startsWith("DISCONNECTED")) {
				//We have a broker disconnection so we have to update our lists
				String[] str = data.getValue().getSpecialMessage().split("\\s+"); //Split the special message into spaces, str[1] => IP, str[2] => PORT
				BrokerObj disconnectedBroker = new BrokerObj(str[1], Integer.parseInt(str[2]));
				Broker.removeBroker(disconnectedBroker);


				//Now we have to change the Broker Provider for every topic that the disconnected Broker was responsible for
				ArrayList<Topic> lostTopics = new ArrayList<>();

				Iterator it = Broker.topicsBroker.entrySet().iterator();
				while (it.hasNext()) {
					HashMap.Entry pair = (HashMap.Entry)it.next();
					if(pair.getValue().equals(disconnectedBroker)) { //If the disconnected broker was responsible for this Topic
						Topic t = (Topic) pair.getKey();
						lostTopics.add(t);
						it.remove();
					}
				}
				System.out.print("\n[!] Broker " + str[1] + ":" + str[2] + " disconnected so we have to redistribute these topics:");
				for(Topic topic : lostTopics)
					System.out.print(" " + topic.getGroupName());
				System.out.println();
				//Now we have to redistribute these topics to the appropriate broker
				Broker.addTopicsToBrokerHashMap(lostTopics);

				//Let's send our new interested list to broker
				out.writeUTF("UPDATE_INTERESTED_TOPICS");
				out.flush();

				ArrayList<Topic> newList = new ArrayList<>();

				BrokerObj thisBroker = new BrokerObj(socket.getLocalAddress().getHostAddress(), socket.getLocalPort());
				it = Broker.topicsBroker.entrySet().iterator();
				while (it.hasNext()) {
					HashMap.Entry pair = (HashMap.Entry)it.next();
					if((pair.getValue()).equals(thisBroker))
						newList.add((Topic) pair.getKey());
				}

				out.writeObject(newList);
				out.flush();

				System.out.print("\nWe are now interested in these topics: ");
				for(Topic topic : newList)
					System.out.print(" " + topic.getGroupName());
				System.out.println();
				return;
			}


			//We got data so we have to forward it to the Subscribers
			for(Subscriber sub : Broker.subscribers) {

				for(Topic topic : sub.getInterestedTopics()) {
					if(topic.getGroupName().equals(data.getKey().getGroupName())) {
						//Subscriber is interested in this key
						if(Broker.openConnections.get(sub) == null)
							System.out.println("Error - no open connection with this Subscriber"); //Bug
						else {
							Broker.openConnections.get(sub).sendData(data);
						}
					}
				}
			}

		} catch(ClassCastException e) {
			System.out.println("Error - Unknown object received.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
