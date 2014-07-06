package br.com.net.bigdata.mapreduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.TimeZone;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.Period;

import br.com.net.bigdata.common.WeekSpec;

public class TestMapReduceAggrDhcp {

	/**
	 * @param args
	 */
	Path netFilePath;
	Charset charset;
	private static String SEPERATOR = ",";
	
	public TestMapReduceAggrDhcp(){
		
		//Objetos para calcular os periodo para duplicação;
		Date enLastEnTime = null;
				
		TimeZone tz = TimeZone.getTimeZone("America/Sao_Paulo");  
	    TimeZone.setDefault(tz);
		
		TreeMap<Date, String> recordSort = new TreeMap<Date, String>();
		HashMap<String,Date[]> dataContextMap = new HashMap<String,Date[]>();
		
		netFilePath = Paths.get("C:/Users/Piva/Documents/ModeloCSVNet/ipaudits_2014.02.17teste2.csv");
		charset = Charset.forName("ISO-8859-1");
		
		//Especificando a semana do registro.
		WeekSpec ws = new WeekSpec();
		SimpleDateFormat sdfNet     = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy", Locale.US);
		SimpleDateFormat sdfParseBD = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
		
		Date[] dates  = new Date[2];
		String valueTextSort = "";
		
		BufferedReader buffR;  
		try {
			buffR = Files.newBufferedReader(netFilePath, charset);
			String text;  
        	while ((text = buffR.readLine ()) != null ) {
        		        		                                              
        		//String[] tokens = StringUtils.split(text.replaceAll("\"", ""), SEPERATOR);
        		//Deslocando alguns arquivos
        		String[] tokens = text.replaceAll("\"", "").split(",");
        		        		
        		//Alterar para joda
        		try{
	    			Date st_time = sdfNet.parse(tokens[0]);
	    			Date en_time = sdfNet.parse(tokens[1]);
	    			
	    			String ip_address = tokens[2];
	        		String mac_address = tokens[4];
	        		String action = tokens[6];
	        		
	        		String texto = sdfParseBD.format(en_time) + ',' + ip_address + ',' + mac_address + ',' + action;
	        		recordSort.put(st_time,texto);
	        		
        		}catch (ParseException e) {
        			// TODO Auto-generated catch block
        			e.printStackTrace();
        		}
        	}
        		
		}catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Efetuar o processamento com o loop ordenado por data de start
		for (Date stSortTime : recordSort.keySet()){
				
						
			String[] valueSort = recordSort.get(stSortTime).split(",");
			
			valueTextSort = "	|" + valueSort[1] + "	|" + valueSort[2] + "	|" + valueSort[3];
			
			Date enSortTime = null;
			try {
				enSortTime = sdfParseBD.parse(valueSort[0]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//VERIFICA SE HOUVE INTERRUPÇÃO DO SERVIÇO, CASO O START DO REGISTRO FOR MAIOR DO TERMINO DO REGISTRO ANTERIOR
			if(enLastEnTime != null && enLastEnTime.before(stSortTime)){
				
				//Encontrou um intervalo, joga os registros no context e inicia uma nova pesquisa
				for (String nameWeek : dataContextMap.keySet()){
					
					Date[] datesAggr = dataContextMap.get(nameWeek);
					System.out.println( datesAggr[0] + " - " + datesAggr[1] + " - " + valueTextSort);			
				
				}
				dataContextMap.clear();				
			}
			
			//APLICA-SE A REGRA DE TRANSBORDO
			ws.setReferenceDate(stSortTime);
			if(enSortTime.after(ws.getLastDate())){
				
				//DATA INICIO DO REGISTRO - DATA FIM DA SEMANA
				if(dataContextMap.containsKey(ws.getNameWeek())){
					dates = dataContextMap.get(ws.getNameWeek());
					dataContextMap.remove(ws.getNameWeek());
					if(dates[0].after(stSortTime)){
						dates[0] = stSortTime;
					}
					if(dates[1].before(ws.getLastDate())){
						dates[1] = ws.getLastDate();
					}
					dataContextMap.put(ws.getNameWeek(),dates);
				}else{
					dates = new Date[] {stSortTime,ws.getLastDate()};
					dataContextMap.put(ws.getNameWeek(),dates);					
				}
					
				//System.out.println(sdfParseBD.format(stSortTime) + " - " + sdfParseBD.format(ws.getLastDate()) + valueTextSort);
				
				int row = 0; 
				do{
					ws.setReferenceDate(ws.getFirstDayofNextWeek());
					if(enSortTime.after(ws.getLastDate())){
						
						//DATA INICIO DA SEMANA - DATA FIM DA SEMANA
						if(dataContextMap.containsKey(ws.getNameWeek())){
							dates = dataContextMap.get(ws.getNameWeek());
							dataContextMap.remove(ws.getNameWeek());
							if(dates[0].after(ws.getFirstDate())){
								dates[0] = ws.getFirstDate();
							}
							if(dates[1].before(ws.getLastDate())){
								dates[1] = ws.getLastDate();
							}
							dataContextMap.put(ws.getNameWeek(),dates);											
						}else{
							dates = new Date[] {ws.getFirstDate(),ws.getLastDate()};
							dataContextMap.put(ws.getNameWeek(),dates);								
						}
						
						//System.out.println(sdfParseBD.format(ws.getFirstDate()) + " - " + sdfParseBD.format(ws.getLastDate()) + valueTextSort);
						row = 0;
					}else{
						
						//DATA INICIO DA SEMANA - DATA FIM DO REGISTRO
						if(dataContextMap.containsKey(ws.getNameWeek())){
							dates = dataContextMap.get(ws.getNameWeek());
							dataContextMap.remove(ws.getNameWeek());
							if(dates[0].after(ws.getFirstDate())){
								dates[0] = ws.getFirstDate();
							}
							if(dates[1].before(enSortTime)){
								dates[1] = enSortTime;
							}
							dataContextMap.put(ws.getNameWeek(),dates);											
						}else{
							dates = new Date[] {ws.getFirstDate(),enSortTime};
							dataContextMap.put(ws.getNameWeek(),dates);	
							
						}
						//System.out.println(sdfParseBD.format(ws.getFirstDate()) + " - " + sdfParseBD.format(enSortTime) + valueTextSort);
						row = 1;
					}				
				}while(row == 0);
			}else{
				
				//DATA INICIO DO REGISTRO - DATA FIM DO REGISTRO
				if(dataContextMap.containsKey(ws.getNameWeek())){
					dates = dataContextMap.get(ws.getNameWeek());
					dataContextMap.remove(ws.getNameWeek());
					if(dates[0].after(stSortTime)){
						dates[0] = stSortTime;
					}
					if(dates[1].before(enSortTime)){
						dates[1] = enSortTime;
					}
					dataContextMap.put(ws.getNameWeek(),dates);
				}else{
					dates = new Date[] {stSortTime,enSortTime};
					dataContextMap.put(ws.getNameWeek(),dates);	
				}
				//System.out.println(sdfParseBD.format(stSortTime) + " - " + valueSort[0].toString() + valueTextSort);
			}
			enLastEnTime = enSortTime;			
	    }
		for (String nameWeek : dataContextMap.keySet()){
			
			Date[] datesAggr = dataContextMap.get(nameWeek);
			System.out.println( datesAggr[0] + " - " + datesAggr[1] + " - " + valueTextSort);			
		
		}
		
	}
	
	public static void main(String[] args){
		new TestMapReduceAggrDhcp();
	}	
}
