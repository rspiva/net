package br.com.net.bigdata.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import br.com.net.bigdata.common.WeekSpec;

public class NetDhcpReducer extends Reducer<Text, Text, NullWritable, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {
	    Text temp = new Text();
	    DateTimeFormatter dtf 	= DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
	    SimpleDateFormat  sdf1 	= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
	    WeekSpec ws = new WeekSpec();
	    int numberWeek  = 0;
	    
	    Date[] firstDayPeriod = new Date[48];
	    Date[] lastDayPeriod  = new Date[48];
	    
	    Date firstPeriod, lastPeriod;
		
	    //iniciar daqui
	    for (Text val : values) {
		    //String[] tokens = StringUtils.split(text.replaceAll("\"", ""), SEPERATOR);
			//Deslocando alguns arquivos
	    	String[] tokens = val.toString().split(",");
			
			//Alterar para joda
			DateTime st_time = dtf.parseDateTime(tokens[0]);
			DateTime en_time = dtf.parseDateTime(tokens[1]);
						
			//DateTime dateTime = new DateTime(date);
			//Data de referencia da semana
			
			//PROCESSO DE TRANSBORDO
			//Transhipment process
			ws.setReferenceDate(st_time.toDate());        		
			//comparação se a data de término esta dentro da data da partição(Ultimo dia da semana)
			//cria um novos registros
			
			//quantas semanas devo duplicar
			//A classe periodo do Joda esta co problema, temos que verificar se será utilizada
			Period period = new Period(st_time,en_time);
			numberWeek    = period.getWeeks();
			//System.out.println(numberWeek);
			
			
			if(en_time.toDate().after(ws.getLastDate())){
				
			    ws.setReferenceDate(st_time.toDate());
				firstPeriod = st_time.toDate();
				lastPeriod  = ws.getLastDate();
				
				for(int i = 0; i <= numberWeek; ++i){
					
					if(numberWeek != i){
						
						if(firstDayPeriod[i] == null){
							firstDayPeriod[i] = firstPeriod;							
						}							
						if(firstDayPeriod[i].after(firstPeriod)){
							firstDayPeriod[i] = firstPeriod;
						}
						
						if(lastDayPeriod[i]==null){
							lastDayPeriod[i] = lastPeriod;						
						}						
						if(lastDayPeriod[i].before(lastPeriod)){
							lastDayPeriod[i] = lastPeriod;
						}
						
					}else{
						
						//tratamento o ultimo loop
						if(firstDayPeriod[i] == null){
							firstDayPeriod[i] = firstPeriod;							
						}						
						if(firstDayPeriod[i].after(firstPeriod)){
							firstDayPeriod[i] = firstPeriod;
						}
						
						if(lastDayPeriod[i]==null){
							lastDayPeriod[i] = en_time.toDate();						
						}
						if(lastDayPeriod[i].before(en_time.toDate())){
							lastDayPeriod[i] = en_time.toDate();
						}
					}
					
					//Popula variaveis para o próximo loop
					ws.setReferenceDate(ws.getFirstDayofNextWeek());
					firstPeriod = ws.getFirstDate();
					lastPeriod  = ws.getLastDate();
				}    				
			}else{
				
				//Logica do numero maior
				// se a data do looping for menor que a do array
				// atualiza o array
				if(firstDayPeriod[0] == null){
					firstDayPeriod[0] = st_time.toDate();							
				}
				if(firstDayPeriod[0].after(st_time.toDate())){
					firstDayPeriod[0] = st_time.toDate();
				}
				
				// se a data do looping for maior que a do array
				// atualiza o array
				if(lastDayPeriod[0]==null){
					lastDayPeriod[0] = en_time.toDate();						
				}
				if(lastDayPeriod[0].before(en_time.toDate())){
					lastDayPeriod[0] = en_time.toDate();
				}				
			}
			//fim do processo de transbordo
			
	    }
	    
	    String[] rawTokens = key.toString().split(",");
	    
	    for(int i = 0; i <= firstDayPeriod.length; i++){
			if( firstDayPeriod[i] == null){
				break;
			}
			temp.set(sdf1.format(firstDayPeriod[i]) + ";" + sdf1.format(lastDayPeriod[i]) + ";" + rawTokens[0] + ";" + rawTokens[1] + ";" + rawTokens[2]);
		    context.write(NullWritable.get(), temp);
	    }	   
	}
		
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
					throws IOException, InterruptedException {
		super.cleanup(context);
	}
}