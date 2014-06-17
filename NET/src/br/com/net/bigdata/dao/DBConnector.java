package br.com.net.bigdata.dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Generic DAO. Can be extended for specific implementation.
 * 
 * @author Garima Dosi
 *
 */
abstract class DBConnector {
	
    protected Statement stmt=null;
    protected Connection con=null;
    

        public boolean createConnection(final String driverName,
			final String connectionString) throws ClassNotFoundException, SQLException,
			IOException {
		Class.forName(driverName);
		con = DriverManager.getConnection(connectionString);
		if (null != con)
			stmt = con.createStatement();
		if (null != stmt)
			return true;
		return false;
	}
	
	/**
	 * Closes connection to the database
	 */
	public void closeConn(){
		try{
			if(null!=stmt){
				stmt.close();
			}
			if(null!=con){
				con.close();
                                con = null;
			}
		}catch(SQLException e){
			//LOG.error("",e);
		}
	}
	
	/**
	 * Executes the specified sql
	 * 
	 * @param sql	SQL
	 * @return		status of execution - true/false
	 * @throws 		SQLException
	 */
	public boolean execute(String sql) throws SQLException {
		//LOG.debug(sql);
		return stmt.execute(sql);
	}
        
        	/**
	 * Executes the specified sql
	 * 
	 * @param sql	SQL
	 * @return		status of execution - true/false
	 * @throws 		SQLException
	 */
	public int executeUpdate(String sql) throws SQLException {
		//LOG.debug(sql);
		return stmt.executeUpdate(sql);
	}

	/**
	 * Executes query (for SELECT queries) 
	 * 
	 * @param sql	SELECT sql
	 * @return		<code>ResultSet</code> result of the executed query
	 * @throws 		SQLException
	 */
	public ResultSet executeSelect(String sql) throws SQLException{
		//LOG.debug(sql);
		return stmt.executeQuery(sql);
	}
	
	/**
	 * Drops a given table
	 * 
	 * @param tableName	table to be dropped	
	 * @throws			SQLException
	 */
	public void dropTable(String tableName) throws SQLException{
		String tmp="DROP TABLE IF EXISTS "+tableName;
		execute(tmp);
	}
	
	/**
	 * Get the statement object
	 * 
	 * @return <code>Statement</code>
	 */
	public Statement getStatement(){
		return stmt;
	}

}
