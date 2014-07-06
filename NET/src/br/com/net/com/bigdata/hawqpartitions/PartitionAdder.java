package br.com.net.com.bigdata.hawqpartitions;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import br.com.net.bigdata.dao.HawqConnector;


public class PartitionAdder {

    private static final boolean HAWQ_AUX_USER = true;
    PartitionController pc = new PartitionController();
      
    //recebe uma lista de parti��o e as cria no banco
    public void createPartitions(List<PartitionSpec> partitionsToAdd){
        
    	HawqConnector hawqConnector;
    	
    	try {
    		hawqConnector = new HawqConnector(HAWQ_AUX_USER);
    		for (PartitionSpec partition : partitionsToAdd) {
    			//verifica se parti��o existe
    			if(!pc.isPartitionExists(partition)){
    				hawqConnector.execute(partition.getSqlPartitionAdder());
    				System.out.println("Ok incluida parti��o");
    			}else{    				
    				System.out.println("parti��o j� existe");
    			}
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