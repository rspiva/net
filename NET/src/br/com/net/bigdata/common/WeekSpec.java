package br.com.net.bigdata.common;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class WeekSpec {
	
	Date firstDate;
	Date lastDate;
	Date referenceDate;
	Date firstDayofNextWeek;
	SimpleDateFormat sdfNamePartition = new SimpleDateFormat("yyyyMMdd");
	
	public void setReferenceDate(Date referenceDate){
		this.referenceDate = referenceDate;
		calculeDateWeekSpec(this.referenceDate);
		
	}
	
	public Date getFirstDate() {
		return this.firstDate;
	}

	public Date getLastDate() {
		return this.lastDate;
	}
	
	public Date getFirstDayofNextWeek() {
		return this.firstDayofNextWeek;
	}
	
	public int getCountWeek(Date start, Date end){
		
		int MILLIS_IN_WEEK = 604800000;  
		  
        Calendar c1 = Calendar.getInstance();  
        c1.setTime(start);  
        c1.set(Calendar.MILLISECOND, 0);  
        c1.set(Calendar.SECOND, 0);  
        c1.set(Calendar.MINUTE, 0);  
        c1.set(Calendar.HOUR_OF_DAY, 0);  
  
        Calendar c2 = Calendar.getInstance();  
        c2.setTime(end);  
        c2.set(Calendar.MILLISECOND, 0);  
        c2.set(Calendar.SECOND, 0);  
        c2.set(Calendar.MINUTE, 0);  
        c2.set(Calendar.HOUR_OF_DAY, 0);
                
        return (int) (Math.floor(c2.getTimeInMillis() - c1.getTimeInMillis()) / MILLIS_IN_WEEK);  		
	}	
	
	private void calculeDateWeekSpec(Date referenceDate){
		
		this.referenceDate = referenceDate;
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setFirstDayOfWeek(Calendar.SUNDAY);
		calendar.setTime(referenceDate);
		
		//First Date
		calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		calendar.set(Calendar.AM_PM, Calendar.AM);
		calendar.set(Calendar.HOUR, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		
		this.firstDate = calendar.getTime();
		
		//last Date
		calendar.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY);
		calendar.set(Calendar.AM_PM, Calendar.PM);
		calendar.set(Calendar.HOUR, 11);
		calendar.set(Calendar.MINUTE, 59);
		calendar.set(Calendar.SECOND, 59);
		
		this.lastDate = calendar.getTime();
		
		//datafinal da semana e acrescenta 1 segundo
		calendar.add(Calendar.SECOND, 1);
		this.firstDayofNextWeek = calendar.getTime();
				
	}
	
	public String getNameWeek(){
		return sdfNamePartition.format(this.firstDate) +  '_' + sdfNamePartition.format(this.lastDate);
	}
}
