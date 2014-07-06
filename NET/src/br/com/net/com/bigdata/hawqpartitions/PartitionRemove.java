package br.com.net.com.bigdata.hawqpartitions;

import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import br.com.net.bigdata.common.PropertiesLoader;
import br.com.net.bigdata.common.WeekSpec;
import br.com.net.bigdata.dao.HawqConnector;

public class PartitionRemove {
	
	private static final boolean HAWQ_AUX_USER = true;
	String nameSchema = PropertiesLoader.getValue("schema");
	WeekSpec ws = new WeekSpec();
	
	SimpleDateFormat sdfNamePartition = new SimpleDateFormat("yyyyMMdd");
	SimpleDateFormat sdfRange         = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	PartitionController pc            = new PartitionController();
	 
	Date dateStart, dateEnd;
		
	public PartitionRemove(Date date, String table){
		
		//Levantando periodo da Partição
		
		ws.setReferenceDate(date);
		
		dateStart = ws.getFirstDate();
		dateEnd   = ws.getLastDate();
		
		String namePartition = sdfNamePartition.format(ws.getFirstDate()) +  '_' + sdfNamePartition.format(ws.getLastDate());
		String startRange = "'" + sdfRange.format(dateStart) + "'::timestamp with time zone";
		String endRange   = "'" + sdfRange.format(dateEnd )  + "'::timestamp with time zone";
				
		//Definindo partição
		PartitionSpec partition = new PartitionSpec(startRange, 
				endRange, 
				namePartition,
				nameSchema, 
				table);
		
		//Verifica se partição existe
		if(pc.isPartitionExists(partition)){
	    	
	    	HawqConnector hawqConnector;
	    	try {
	    		hawqConnector = new HawqConnector(HAWQ_AUX_USER);
	    	    hawqConnector.execute(partition.getSqlPartitionDrop());
	    		hawqConnector.closeConn();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
	    		e.printStackTrace();
	    	}
	    	
	    }else{	    	
	    	System.out.println("Partição a ser deletada não existe");	    	
	    }
	}
}
