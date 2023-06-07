package cl.security.mdd.dto;

public class Messages {
	String interfaceIP;
	int interfacePort;
	String interfaceParams;

	public Messages() {

	}

	public Messages(String interfaceIP, int interfacePort, String interfaceParams) {
		this.interfaceIP = interfaceIP;
		this.interfacePort = interfacePort;
		this.interfaceParams = interfaceParams;
	}

	public String getInterfaceIP() {
		return interfaceIP;
	}

	public void setInterfaceIP(String interfaceIP) {
		this.interfaceIP = interfaceIP;
	}

	public int getInterfacePort() {
		return interfacePort;
	}

	public void setInterfacePort(int interfacePort) {
		this.interfacePort = interfacePort;
	}

	public String getInterfaceParams() {
		return interfaceParams;
	}

	public void setInterfaceParams(String interfaceParams) {
		this.interfaceParams = interfaceParams;
	}

	@Override
	public String toString() {
		return "Messages [interfaceIP=" + interfaceIP + ", interfacePort=" + interfacePort + ", interfaceParams="
				+ interfaceParams + "]";
	}
}
