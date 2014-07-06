package br.com.net.bigdata.model;

public class Cdr extends AccessLog{
		
	private String subscriberIP, 
			       userName, 
			       userCpf, 
			       contextName, 
			       macAdrress,
			       nasIPAdrress,
			       netDeviceModel,
			       netDeviceOS,
			       netDeviceBrowser;

	public String getSubscriberIP() {
		return subscriberIP;
	}

	public void setSubscriberIP(String subscriberIP) {
		this.subscriberIP = subscriberIP;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getUserCpf() {
		return userCpf;
	}

	public void setUserCpf(String userCpf) {
		this.userCpf = userCpf;
	}

	public String getContextName() {
		return contextName;
	}

	public void setContextName(String contextName) {
		this.contextName = contextName;
	}

	public String getMacAdrress() {
		return macAdrress;
	}

	public void setMacAdrress(String macAdrress) {
		this.macAdrress = macAdrress;
	}

	public String getNasIPAdrress() {
		return nasIPAdrress;
	}

	public void setNasIPAdrress(String nasIPAdrress) {
		this.nasIPAdrress = nasIPAdrress;
	}

	public String getNetDeviceModel() {
		return netDeviceModel;
	}

	public void setNetDeviceModel(String netDeviceModel) {
		this.netDeviceModel = netDeviceModel;
	}

	public String getNetDeviceOS() {
		return netDeviceOS;
	}

	public void setNetDeviceOS(String netDeviceOS) {
		this.netDeviceOS = netDeviceOS;
	}

	public String getNetDeviceBrowser() {
		return netDeviceBrowser;
	}

	public void setNetDeviceBrowser(String netDeviceBrowser) {
		this.netDeviceBrowser = netDeviceBrowser;
	}
		
}
