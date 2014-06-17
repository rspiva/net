package br.com.net.bigdata.controller;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import br.com.net.bigdata.dao.HawqConnector;
import br.com.net.bigdata.model.AccessLog;
import br.com.net.bigdata.model.Cdr;


public class CdrController{
	
	private static char SEPERATOR = ';';
	
	Path netFilePath;
	Charset charset;
	List<Cdr> cdrList;
	
	public List getCdrList(){
		return this.cdrList;
	}

	public AccessLog getRecordObject(String record) {
		String[] recordsStrings = StringUtils.split(record, SEPERATOR);
		
		Cdr cdr = new Cdr();
		cdr.setSubscriberIP(recordsStrings[5]); //Subscriber-IP
		cdr.setUserName(recordsStrings[3]);      //User-Name
		cdr.setUserCpf(recordsStrings[4]);		 //User-CPF
		cdr.setContextName(recordsStrings[1]);   //Context-Name
		cdr.setMacAdrress(recordsStrings[6]);    //Subs-MAC-address
		cdr.setNasIPAdrress(recordsStrings[7]);   //NAS-IP-address
		
		SimpleDateFormat sdf1= new SimpleDateFormat("ddMMyyyy");
		
		try {
			cdr.setStartTime(sdf1.parse(recordsStrings[8])); //Session_Start_time
			cdr.setEndTime(sdf1.parse(recordsStrings[10]));  //Session_End_time
		} catch (ParseException e) {
			System.out.println("Formato incorreto");
			e.printStackTrace();
		}
		
		//cdr.setNetDeviceModel(recordsStrings[16]); 		//NetDeviceModel
		//cdr.setNetDeviceOS(recordsStrings[17]);			//NetDeviceOS
		//cdr.setNetDeviceBrowser(recordsStrings[18]);      //NetDeviceBrowser
		
		return cdr;
	}
	
	public void getListAccessLog(){
		
		cdrList = new ArrayList<Cdr>();
		netFilePath = Paths.get("C:/Users/Piva/Documents/ModeloCSVNet/CDR_NETCOMBOCORP.COM.BR_20130718_00310.csv");
		charset = Charset.forName("ISO-8859-1");
		
		BufferedReader buffR;  
		try {
			buffR = Files.newBufferedReader(netFilePath, charset);
			String text;  
        	while ((text = buffR.readLine ()) != null ) {
        		cdrList.add((Cdr) getRecordObject(text));
			    //System.out.println(cliente.getAluno());  
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void writesRecords(){
		// TODO Auto-generated method stub
		StringBuilder sqlInput = new StringBuilder();
		sqlInput.append("INSERT INTO TB_CDR (StartTime,EndTime,SubscriberIP,UserName,CpfName,ContextName,MacAddress,NASIPAddress,NetDeviceModel,NetDeviceOS,NetDeviceBrowser) VALUES \n");
		for(Cdr cdrT : cdrList){
			
			SimpleDateFormat sdf1= new SimpleDateFormat("yyyy-MM-dd");
			
			sqlInput.append("("); 
			sqlInput.append(sdf1.format(cdrT.getStartTime()) + ",");
			sqlInput.append(sdf1.format(cdrT.getEndTime())   + ",");
			sqlInput.append(cdrT.getSubscriberIP()           + ",");
			sqlInput.append(cdrT.getUserName()               + ",");
			sqlInput.append(cdrT.getUserCpf()                + ",");
			sqlInput.append(cdrT.getContextName()            + ",");
			sqlInput.append(cdrT.getMacAdrress()             + ",");
			sqlInput.append(cdrT.getNasIPAdrress()           + ",");
			sqlInput.append(cdrT.getNetDeviceModel()         + ",");
			sqlInput.append(cdrT.getNetDeviceOS()            + ",");
			sqlInput.append(cdrT.getNetDeviceBrowser()            );
			
			sqlInput.append("),\n");
			
		}
		sqlInput.deleteCharAt(sqlInput.lastIndexOf(","));
		System.out.println(sqlInput);
		
		HawqConnector hawqConnector;
		try {
			hawqConnector = new HawqConnector(true);
			boolean isScriptExecuted = hawqConnector.execute(sqlInput.toString());
			
			if(isScriptExecuted){
				hawqConnector.commit();
			}else{
				hawqConnector.rollback();
			}
			hawqConnector.closeConn();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
	}
}



