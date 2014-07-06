package br.com.net.bigdata.mapreduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import br.com.net.bigdata.common.WeekSpec;

public class TesteMapreduce {
	
	Path netFilePath;
	Charset charset;
	private static char SEPERATOR = ',';
	
	
	public TesteMapreduce(){
		String key 		= "";
		String value 	= "";
		int numberWeek  = 0;
		
		//Objetos para calcular os periodo para duplicação;
		Date firstDayPeriod;
		Date lastDayPeriod;
				
		TimeZone tz = TimeZone.getTimeZone("America/Sao_Paulo");  
        TimeZone.setDefault(tz);
		
		TreeMap<String, String> recordRepo = new TreeMap<String, String>();
		netFilePath = Paths.get("C:/Users/Piva/Documents/ModeloCSVNet/ipaudits_2014.02.17teste.csv");
		charset = Charset.forName("ISO-8859-1");
		
		
		//Especificando a semana do registro.
		WeekSpec ws = new WeekSpec();
		
		//Definindo Data Locale.US
		DateTimeFormatter dtf = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss yyyy");
		dtf = dtf.withLocale(Locale.US);
				
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
		
		BufferedReader buffR;  
		try {
			buffR = Files.newBufferedReader(netFilePath, charset);
			String text;  
        	while ((text = buffR.readLine ()) != null ) {
        		
        		                                              
        		//String[] tokens = StringUtils.split(text.replaceAll("\"", ""), SEPERATOR);
        		//Deslocando alguns arquivos
        		String[] tokens = text.replaceAll("\"", "").split(",");
        		
        		//Alterar para joda
    			DateTime st_time = dtf.parseDateTime(tokens[0]);
    			DateTime en_time = dtf.parseDateTime(tokens[1]);
    			
    			//DateTime dateTime = new DateTime(date);
    			//Data de referencia da semana
    			
    			
    			String ip_address = tokens[2];
        		String mac_address = tokens[4];
        		String action = tokens[6];
        		
        		key 		= mac_address + "," + ip_address + "," + action;
        		
        		//PROCESSO DE TRANSBORDO
        		//Transhipment process
        		ws.setReferenceDate(st_time.toDate());        		
    			//comparação se a data de término esta dentro da data da partição(Ultimo dia da semana)
    			//cria um novos registros        		
    			if(en_time.toDate().after(ws.getLastDate())){
    			
    			    //quantas semanas devo duplicar
    				//A classe periodo do Joda esta co problema, temos que verificar se será utilizada
    				numberWeek    = ws.getCountWeek(st_time.toDate(), en_time.toDate());
    				System.out.println(numberWeek);
    				
    				ws.setReferenceDate(st_time.toDate());
    				firstDayPeriod = st_time.toDate();
    				lastDayPeriod  = ws.getLastDate();
    				
    				//preenche semana 0
    				value 	    = sdf1.format(firstDayPeriod) + "," + sdf1.format(lastDayPeriod);
            		System.out.println("Key (" + key + ") Value: " + value);
    				
    				for(int i = 0; i <= numberWeek; i++){
    					if(numberWeek != 0){
	    					//quebra data inicio
		    				value 	    = sdf1.format(firstDayPeriod) + "," + sdf1.format(lastDayPeriod);
		            		System.out.println("Key (" + key + ") Value: " + value);
    					
		            		//Popula variaveis para o próximo loop
    					}
    					ws.setReferenceDate(ws.getFirstDayofNextWeek());
    					firstDayPeriod = ws.getFirstDate();
        				lastDayPeriod  = ws.getLastDate();
    				}
    				
    				//preenche semana final
    				value 	    = sdf1.format(firstDayPeriod) + "," + sdf1.format(en_time.toDate());
            		System.out.println("Key (" + key + ") Value: " + value);
    				
    			}else{
    				
    				value 	    = sdf1.format(st_time.toDate()) + "," + sdf1.format(en_time.toDate());
    				System.out.println("Key (" + key + ") Value: " + value);
    				
    			}
    			
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
	
	public static void main(String[] args){
		new TesteMapreduce();
		
		// teste da classe de determinar semana
		//WeekSpec ws = new WeekSpec(new Date());
		/*
		SimpleDateFormat sdf1 = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
		Date data = null;
		try {
			data = sdf1.parse("07/08/2014 08:08:08");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		WeekSpec ws = new WeekSpec(data);
		System.out.println(sdf1.format(ws.getFirstDate()) + " + " + sdf1.format(ws.getLastDate()));
		*/
		
		
	}

}
