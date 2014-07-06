package br.com.net.com.bigdata.hawqpartitions;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;

import br.com.net.bigdata.common.PropertiesLoader;

public class PartitionRemoveThreeYearsAgo {
	
	
	/**
	 * @param String
	 */
	public static void main(String[] args) {
		
		Date dateCurrent    				= null;
		Date date3yearsAgo					= null;
		SimpleDateFormat sdfParameter     	= new SimpleDateFormat("yyyy-MM-dd");
		try {
			dateCurrent = sdfParameter.parse(args[0]);
		} catch (ParseException e) {
			System.out.println("Parametro de data incorreto");
			System.exit(0);
		}
		
		//Calcula a data de referencia com a data de 3 anos atras
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTime(dateCurrent);
		calendar.add(Calendar.YEAR, -3);
		date3yearsAgo = calendar.getTime();
				
		//Levanta quais tabelas terão a partição deletada
		String nameTables = PropertiesLoader.getValue("partitiontables");
		String[] tables = nameTables.split(";");
		for(String table : tables){
			new PartitionRemove(date3yearsAgo, table);
		}
	}
}
