package distributed.broker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class BrokerServerSocket extends Thread {
	static ObjectOutputStream connectionToPublisher;
	int port;
	public void run() {
		openServer(port);
	}

	public void openServer(int port) {
		ServerSocket providerSocket = null;
		Socket socket = null;
		try {
			//We have incoming connections ONLY from Subscribers
			providerSocket = new ServerSocket(port);
			System.out.println("Started Broker on port " + port);
			while (true) {
				socket = providerSocket.accept();

				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

				//New connection! We have to wait for message to determine if it's PUBLISHER or SUBSCRIBER
				String command = in.readUTF();
				if(command.equals("PUBLISHER")) {
					//Connection from publisher!
					System.out.println("[+] Got connection from Publisher!");
					connectionToPublisher = out;
					BrokerPubConnection connection = new BrokerPubConnection(socket, out, in);
					connection.start();
				} else if(command.equals("SUBSCRIBER")) {
					//Connection from subscriber!
					System.out.println("[+] Got connection from Subscriber!");
					BrokerSubConnection connection = new BrokerSubConnection(socket, out, in);
					connection.start();
				} else
					System.out.println("Error - received: " + command); //Debug

			}

		} catch (IOException ioException) {
			System.out.println("Error occurred.");
			ioException.printStackTrace();
			return;
		} finally {
			try {
				providerSocket.close();
			} catch (IOException ioException) {
				ioException.printStackTrace();
			} catch(NullPointerException e) {
				e.printStackTrace();
			}
		}
	}

	BrokerServerSocket(int port) {
		this.port = port;
	}
}
