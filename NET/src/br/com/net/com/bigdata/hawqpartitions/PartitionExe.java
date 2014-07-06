package br.com.net.com.bigdata.hawqpartitions;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import br.com.net.bigdata.common.PropertiesLoader;
import br.com.net.bigdata.common.WeekSpec;

public class PartitionExe {
	
	String nameSchema = PropertiesLoader.getValue("schema");
	String nameTables = PropertiesLoader.getValue("partitiontables");
	Date dateStart;
	Date dateEnd;
	
		
	public static void main(String[] args) {
		new PartitionExe().getCaulatePartition( args);
	
	}
	
	//executa as partições criadas
	void getCaulatePartition(String[] dates){
		
		//Create list partition
		List<PartitionSpec>  listPartition = new ArrayList<PartitionSpec>();
		WeekSpec ws = new WeekSpec();
		
		PartitionAdder parttionAdder = new PartitionAdder();
		SimpleDateFormat sdfNamePartition = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdfParameter     = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdfRange         = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		//tabelas a serem parcionadas
		String[] tables =  nameTables.split(";");
		
		for(String table : tables){
			for(String date: dates){
				
				try {
					ws.setReferenceDate(sdfParameter.parse(date));
				} catch (ParseException e) {
					System.out.println("Data mencionada no Shell Script não esta no formato correto");
					e.printStackTrace();
				}
				
				//Definindo partição
				dateStart = ws.getFirstDate();
				dateEnd   = ws.getLastDate();
				
				String namePartition = sdfNamePartition.format(ws.getFirstDate()) +  '_' + sdfNamePartition.format(ws.getLastDate());
				String startRange = "'" + sdfRange.format(dateStart) + "'::timestamp with time zone";
				String endRange   = "'" + sdfRange.format(dateEnd )  + "'::timestamp with time zone";
				
					
				PartitionSpec partitionSpec = new PartitionSpec(startRange, 
						                                        endRange, 
																namePartition, //
																nameSchema, 
																table);
				//System.out.println(partitionSpec.getSqlPartitionAdder());
				listPartition.add(partitionSpec);
			}
		}
		parttionAdder.createPartitions(listPartition);
	}
}
