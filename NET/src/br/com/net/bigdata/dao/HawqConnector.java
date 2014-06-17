/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.net.bigdata.dao;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

//import org.apache.log4j.Logger;

/**
 *
 * @author Devayon
 */
public class HawqConnector extends DBConnector {

    //private static final Logger LOG = Logger.getLogger(HawqConnector.class);
    public static final String DRIVER = "org.postgresql.Driver";
    private static HawqConnector instantiated = null;
    private String dbname;

    public String getDbname() {
        return dbname;
    }
    
    public void setInstance( HawqConnector conn ) {
        instantiated = conn;
    }

    /**
     * Makes a new Hawq connection. If you want to reuse connections, use getConnection() instead of this constructor
     *
     * @throws IOException
     * @throws SQLException
     */
    public HawqConnector( boolean isAuxiliaryData ) throws IOException, SQLException {
        super();
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException ex) {
            throw new IOException("HAWQ Postgres Driver not in classpath", ex);
        }
        String username = "root";
        String password = "password";
        if (username != null) {
            if (password != null) {
                con = DriverManager.getConnection("jdbc:postgresql://" + getDBString(), username, password);
            } else {
                con = DriverManager.getConnection("jdbc:postgresql://" + getDBString(), username, null);
            }
        } else {
            con = DriverManager.getConnection("jdbc:postgresql://" + getDBString());
        }
        stmt = con.createStatement();
    }

    HawqConnector(Connection con) throws IOException, SQLException {
        this.con = con;
        stmt = con.createStatement();
    }

    public Connection getJdbcConnection() {
        return con;
    }

    public static String getDBString() throws IOException {
    	String host = "host";
    	String port = "5503";
    	String database = "database";
    	
    	//String host = PropConfig.getProperty(Constants.PROP_HAWQ_MASTER);
        //String port = PropConfig.getProperty(Constants.PROP_HAWQ_PORT);
        //String database = PropConfig.getProperty(Constants.PROP_HAWQ_DBNAME);
        return host + ":" + port + "/" + database;
    }

    /**
     * Sets the hawq specific driver, connectionString, user name, password. Is a singleton.
     *
     * @throws SQLException
     * @throws IOException
     */
    public static HawqConnector getConnection( boolean isAuxiliary ) throws IOException, SQLException {
        if (null == instantiated) {
            instantiated = new HawqConnector( isAuxiliary );
        }
        return instantiated;
    }

    public static HawqConnector getConnection(Connection con) throws IOException, SQLException {
        if (null == instantiated) {
            instantiated = new HawqConnector(con);
        }

        return instantiated;
    }

    
    /*
    static String replaceTokens(String sql, Map<String, String> keyValues) {
        if (keyValues != null && !keyValues.isEmpty() && sql != null) {
            for (Map.Entry<String, String> entry : keyValues.entrySet()) {
                sql = sql.replace(entry.getKey(), entry.getValue());
            }
        }
        return sql;
    }
    
    

    public boolean executeCreateScripts(File sqlFile, Map<String, String> tokens) throws IOException, SQLException {
        boolean isScriptExecuted = false;
        if (!sqlFile.isFile()) {
            throw new IOException(sqlFile + " is not a valid file");
        }
        BufferedReader in = new BufferedReader(new FileReader(sqlFile));
        String str;
        StringBuilder sb = new StringBuilder();
        while ((str = in.readLine()) != null) {
            if (!str.startsWith("--")) {
                sb.append(str).append("\n ");
            }
        }
        in.close();
        String sql = replaceTokens(sb.toString(), tokens);
       // LOG.debug(sql);
        stmt.execute(sql);

        isScriptExecuted = true;

        return isScriptExecuted;
    }

    public boolean executeDDLScripts(File sqlFile) throws IOException, SQLException {
        try {
            boolean isScriptExecuted = false;
            if (!sqlFile.isFile()) {
                throw new IOException(sqlFile + " is not a valid file");
            }
            BufferedReader in = new BufferedReader(new FileReader(sqlFile));
            String str;
            StringBuilder sb = new StringBuilder();
            while ((str = in.readLine()) != null) {
                if (!str.startsWith("--")) {
                    sb.append(str).append("\n ");
                }
            }
            in.close();
            String sql = sb.toString();
            //LOG.debug(sql);
            stmt.execute(sql);

            isScriptExecuted = true;

            return isScriptExecuted;
        } catch (SQLException e) {
            System.err.println(e.getLocalizedMessage());
            throw e;
        }
    }

    /**
     * Returns number of rows inserted
     *
     * @param sqlFile
     * @param keyValues
     * @return
     */
    /*
    public int executeInsertScripts(File sqlFile, Map<String, String> keyValues) throws IOException, SQLException {
        BufferedReader in = new BufferedReader(new FileReader(sqlFile));
        String str;
        StringBuilder sb = new StringBuilder();
        while ((str = in.readLine()) != null) {
            if (!str.startsWith("--")) {
                sb.append(str).append("\n ");
            }
        }
        in.close();
        String sql = replaceTokens(sb.toString(), keyValues);
        //LOG.debug(sql);
        return stmt.executeUpdate(sql);
    }

    public void setSavepoint() throws SQLException {
        con.setAutoCommit(false);
    }
    */

    public void rollback() throws SQLException {
        con.rollback();
    }

    public void commit() throws SQLException {
        con.commit();
        con.setAutoCommit(true);
    }
}
