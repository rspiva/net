package br.com.net.bigdata.controller;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import br.com.net.bigdata.common.PropertiesLoader;
import br.com.net.bigdata.common.WeekSpec;
import br.com.net.bigdata.model.AccessLog;
import br.com.net.com.bigdata.hawqpartitions.PartitionAdder;
import br.com.net.com.bigdata.hawqpartitions.PartitionRemove;
import br.com.net.com.bigdata.hawqpartitions.PartitionSpec;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		//new PartitionRemove(date, table)
		/*
		CdrController cc = new CdrController();
		cc.getListAccessLog();
		cc.writesRecords();
		*/
		/*
		PartitionSpec ps = new PartitionSpec("2014-06-03 00:00:00+08",
				                             "2014-06-03 23:59:59+08",
				                             "20140630",
				                             PropertiesLoader.getValue("schema"),
				                             "tb_cdr");
		
		System.out.println(ps.getSqlPartitionDrop());
		/*
		System.out.println(ps.getSqlPartitionAdder());
		List<PartitionSpec> listPartition = new ArrayList<PartitionSpec>();
		listPartition.add(ps);
		
		/*System.out.println(new PartitionAdder().isPartitionExists(ps));*/
		/*
		new PartitionAdder().createPartitions(listPartition);
		*/
		
		
		/*
		System.out.println(PropertiesLoader.getValue("schema"));
		
		SimpleDateFormat sdf1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
		System.out.println(sdf1.format(new Date()));
		*/
		
		
		
		DateTimeFormatter dtfFormatBD = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
		String dataescrita = "2014-07-17 10:14:07";
		
		DateTime dt = dtfFormatBD.parseDateTime(dataescrita);
		//System.out.println(print);
		
		Period period = new Period(new DateTime(new Date()), dt);
	    int numberWeek = period.getWeeks();
		System.out.println("Qtd Semana: " + numberWeek);
		
		Date[] dates = new Date[47];
		dates[0] = new Date();
		dates[1] = dt.toDate();
		
		WeekSpec wc = new WeekSpec();
		wc.setReferenceDate(new Date());
		System.out.println(wc.getNameWeek());
		
		for(Date dts : dates ){
			if(dts == null){
				break;
			}
			System.out.println(dts);
		}
		//return 0;
	}

}
