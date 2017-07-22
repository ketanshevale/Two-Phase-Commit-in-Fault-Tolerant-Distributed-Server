
public class CoordinatorInfo {
	String name;
	String ip;
	int port;
	
	@Override
	public String toString() {
		return "CoordinatorInfo [name=" + name + ", ip=" + ip + ", port=" + port + "]";
	}
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
	public void setPort(int port) {
		this.port = port;
	}
	
}
