
public class ReplicaInfo {
	String name;
	String ip;
	int port;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public int getPort() {
		return port;
	}
	@Override
	public String toString() {
		return "ReplicaInfo [name=" + name + ", ip=" + ip + ", port=" + port + "]";
	}
	public void setPort(int i) {
		this.port = i;
	}
}
