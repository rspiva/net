package br.com.net.bigdata.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import br.com.net.bigdata.model.Cdr;


public class PropertiesLoader {  
  
    private static Properties props;
    public static void getPropertiesLoader(){
    	    
    		Path propertiesFilePath = Paths.get("C:/Net/Config/net_config.properties");
    	    props = new Properties();  
            InputStream in;
			try {
				in = Files.newInputStream(propertiesFilePath);
				props.load(in);  
                in.close();  
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
            
    }  
  
    public static String getValue(String key){
    	    getPropertiesLoader();
            return (String)props.getProperty(key);  
    }  
}  