package distributed.publisher;

import distributed.datastructure.*;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;


public class Publisher extends Thread {

	static ArrayList<PubToBrokerConnection> connections;
	static ArrayList<BrokerObj> brokers;
	static ArrayList<Topic> topics;

    public static void main(String[] args) throws IOException, InterruptedException {
		ReaderForPublisher reader;
		if(args.length == 0) {
			System.out.println("File paths not specified: pub.txt and brok.txt used!!!");
			reader = new ReaderForPublisher(null,null);
		}else {
			//Arguments should be "resources/pubxx.txt resources/brokxxx.txt" .
			reader = new ReaderForPublisher(args[0],args[1]);
		}

		brokers = reader.getBrokerObjs();
		topics = reader.getTopicsInterested();


		connections = new ArrayList<>();

		//Open connection with every broker
		Socket socket;
		for(BrokerObj broker : brokers) {
			socket = new Socket(broker.getIp(), broker.getPort());
			PubToBrokerConnection con = new PubToBrokerConnection(socket, topics, reader);
			con.start();
			connections.add(con);
		}


		while(true) {
			Data last_msg = reader.getLastMessageIfReceived();
			if(last_msg != null) {
				push(last_msg);
				reader.markMessageSent();
			}

			Thread.sleep(200);
		}

    }

    public static void push(Data data) throws IOException {
    	//Data has parameters: (Topic, Value)
		for(PubToBrokerConnection connection : connections) {
			connection.writeObj(data);

		}
	}

}
