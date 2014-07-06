package br.com.net.com.bigdata.hawqpartitions;

public class PartitionSpec{
	
    private String startRange;
    private String endRange;
    private String timezone;
    private String partitionName;
    private String tableName;
    private String schemaName;
        
	//Construtor com o modelo da partição
	public PartitionSpec(String startRange, String endRange,
			String partitionName, String schemaName, String tableName) {
		super();
		this.startRange = startRange;
		this.endRange = endRange;
		this.partitionName = partitionName;
		this.tableName = tableName;
		this.schemaName = schemaName;
	}
	
	public String getPartitionName() {
		return partitionName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getSchemaName() {
		return schemaName;
	}

	//String em sql para adicionar a partição
	public String getSqlPartitionAdder(){
		StringBuilder sql = new StringBuilder();
		sql.append("ALTER TABLE ");
		sql.append(this.schemaName);
		sql.append(".");
		sql.append(this.tableName);
		sql.append(" split default partition start(");
		sql.append(this.startRange);
        sql.append(") end(");
        sql.append(this.endRange);
        sql.append(") ");
        sql.append("INTO (PARTITION \"");
        sql.append(this.partitionName);
        sql.append("\", PARTITION outlying)");
        
		return sql.toString();		
	}
	
	//String em sql para dropar a partição
	public String getSqlPartitionDrop(){
		
		StringBuilder sql = new StringBuilder();
		sql.append("ALTER TABLE ");
		sql.append(this.schemaName);
		sql.append(".");
		sql.append(this.tableName);
		sql.append(" drop partition \"");
		sql.append(this.partitionName);
	    sql.append("\"");
	    
	    return sql.toString();
		
	}
}