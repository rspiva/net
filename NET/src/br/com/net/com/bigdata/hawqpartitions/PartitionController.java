package br.com.net.com.bigdata.hawqpartitions;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import br.com.net.bigdata.dao.HawqConnector;

public class PartitionController {

	private static final boolean HAWQ_AUX_USER = true;
	 
	//verifica se uma partição existe
    public boolean isPartitionExists(PartitionSpec partition){
    	
    	boolean partitionExist = false;
    	
    	StringBuilder sql = new StringBuilder();
		sql.append("select * from pg_partitions where tablename = '");
		sql.append(partition.getTableName());
		sql.append("' and schemaname = '");
		sql.append(partition.getSchemaName());
		sql.append("' and partitionname = '");
		sql.append(partition.getPartitionName());
		sql.append("'");
		//System.out.println(sql.toString());
		HawqConnector hawqConnector;
		ResultSet resultSet;
		
    	try {
    		hawqConnector = new HawqConnector(HAWQ_AUX_USER);
    		resultSet = hawqConnector.executeSelect(sql.toString());
    		if(resultSet.next()){
    			partitionExist = true;    			    			
    		} 		
    		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
    	return partitionExist;
    }

}
