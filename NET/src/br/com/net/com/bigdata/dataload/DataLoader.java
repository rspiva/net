package br.com.net.com.bigdata.dataload;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;





/**
 * 
 * @author Rodrigo Piva
 */
public class DataLoader {
	
	private static char SEPERATOR = ',';
	
	/**
     * Driver method. Arguments: <li>path to directory of files</li> <li>gpfdist
     * url of directory</li> <li>datatype</li>
     */
	public static void main(String[] args){
		
		DataLoader dataLoader = null;
		try {
            String[] dirStrings = StringUtils.split(args[0], SEPERATOR);
            List<File> dirs = new ArrayList<File>();
            for (String dir : dirStrings) {
                dirs.add(new File(dir));
            }
            
            /*
            String[] urls = StringUtils.split(args[2], SEPERATOR);
            if (urls.length != dirs.size()) {
                throw new IOException("Mismatch: there are " + urls.length
                        + " urls configured, but " + dirs.size()
                        + "directories configured");
            }
            */

        } finally {
        }
		
	}

}
