package br.com.net.bigdata.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import br.com.net.bigdata.common.WeekSpec;

public class NetDhcpReducer_v2 extends Reducer<Text, Text, NullWritable, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {
	    Text temp = new Text();
	    TreeMap<Date, Date> recordSort = new TreeMap<Date, Date>();
	    HashMap<String,Date[]> dataContextMap = new HashMap<String,Date[]>();
	    SimpleDateFormat  sdf1 	= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
	    WeekSpec ws = new WeekSpec();
	    
	    Date enLastEnTime = null;
	    Date[] dates  = new Date[2];
	    Date enSortTime;
		
	    String textValue = "";
	    
	    //iniciar daqui
	    for (Text val : values) {
	    	
	    	String[] tokens = val.toString().split(",");
	    	String[] tokenKey = key.toString().split(",");
	    	
	    	Date st_time;
	    	Date en_time;
			try {
				st_time = sdf1.parse(tokens[0]);
				en_time = sdf1.parse(tokens[1]);
				
				String ip_address = tokenKey[0];
	    		String mac_address = tokenKey[1];
	    		String action = tokenKey[2];
	    		
	    		textValue = ip_address + '|' + mac_address + '|' + action;
	    		recordSort.put(st_time,en_time);
			    
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
	    }
	    
	    for (Date stSortTime : recordSort.keySet()){
	    	
	    	enSortTime = recordSort.get(stSortTime);
	    	
	    	//VERIFICA SE HOUVE INTERRUPÇÃO DO SERVIÇO, CASO O START DO REGISTRO FOR MAIOR DO TERMINO DO REGISTRO ANTERIOR
			//JOGA A CLASSE MAP NO CONTEXT E LIMPA A LISTA
	    	if(enLastEnTime != null && enLastEnTime.before(stSortTime)){
				
				//Encontrou um intervalo, joga os registros no context e inicia uma nova pesquisa
				for (String nameWeek : dataContextMap.keySet()){
					
					//inserir dados do context aqui
					Date[] datesAggr = dataContextMap.get(nameWeek);
					temp.set(sdf1.format(datesAggr[0]) + "	|" + sdf1.format(datesAggr[1]) + "	|" + textValue);
				    context.write(NullWritable.get(), temp);
					
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
		    temp.set(sdf1.format(datesAggr[0]) + "	|" + sdf1.format(datesAggr[1]) + "	|" + textValue);
		    context.write(NullWritable.get(), temp);
		}
	}
		
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
					throws IOException, InterruptedException {
		super.cleanup(context);
	}
}