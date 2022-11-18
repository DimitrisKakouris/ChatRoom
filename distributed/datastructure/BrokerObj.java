package distributed.datastructure;

import java.io.Serializable;

import static distributed.broker.Broker.md5;

public class BrokerObj implements Comparable<BrokerObj>, Serializable {
	private final String ip;
	private final int port;
	private String hash;

	public BrokerObj(String ip, int port) {
		this.ip = ip;
		this.port = port;
		this.hash = md5(ip+port);
	}

	@Override
	public int compareTo(BrokerObj broker) {
		return hash.compareTo(broker.getHash());
	}

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof BrokerObj) {
			BrokerObj b = (BrokerObj) o;
			return (ip.equals(b.getIp()) && port == b.getPort());
		}
		return false;
	}
}


