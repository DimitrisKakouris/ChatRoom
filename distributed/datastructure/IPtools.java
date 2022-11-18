package distributed.datastructure;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IPtools {
	public static String getRemoteIP(Socket socket) {
		InetSocketAddress socketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
		InetAddress inetAddress = socketAddress.getAddress();

		return inetAddress.getHostAddress();
	}

	public static String getLocalIp() throws IOException {
		String command = null;
		if(System.getProperty("os.name").equals("Linux"))
			command = "ifconfig";
		else
			command = "ipconfig";
		Runtime r = Runtime.getRuntime();
		Process p = r.exec(command);
		Scanner s = new Scanner(p.getInputStream());

		StringBuilder sb = new StringBuilder("");
		while(s.hasNext())
			sb.append(s.next());
		String ipconfig = sb.toString();
		Pattern pt = Pattern.compile("192\\.168\\.[0-9]{1,3}\\.[0-9]{1,3}");
		Matcher mt = pt.matcher(ipconfig);
		mt.find();
		return mt.group();
	}
}
