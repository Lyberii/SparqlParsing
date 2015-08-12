package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;



public class MySql {
	
    private static Connection conn;
    
    public MySql(){
    	connect();
    }
    public void connect(){
    	Properties info = new Properties();
		info.setProperty("proxool.maximum-connection-count", "20");
		info.setProperty("proxool.house-keeping-test-sql", "select CURRENT_DATE");
		info.setProperty("user", "root");
		info.setProperty("password", "root@ecust4poa");
		info.setProperty("characterEncoding", "utf-8");
		String alias = "test";
		String driverClass = "com.mysql.jdbc.Driver";
		String driverUrl = "jdbc:mysql://192.168.1.9:19131/ssco_zhoushan";
		String url = "proxool." + alias + ":" + driverClass + ":" + driverUrl;
        try {
			Class.forName("org.logicalcobwebs.proxool.ProxoolDriver");
			conn = DriverManager.getConnection(url,info);
		}catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    /**
     * Function: 	retain table set according to the attributes given
     * @param attributes 	attribute sets
     * @return 				Map<id，Parameter<tableName,attributeName>>
     */
	public Set<String> getTableByAttributeName(String attribute,String domain, HashMap<Long,String> pidValue, StringBuffer strb) throws SQLException{
		Set<String> tables=new HashSet<String>();
         try { 
        	 if(conn==null || conn.isClosed()){
        		 connect();
        	 }
        	  Statement statement = conn.createStatement();
        	  String sql = "select type, data_type, id, name from attribute_definition where name='"+attribute+"' and domain_value='"+domain+"';";
        	  ResultSet rs = statement.executeQuery(sql);
        	  while(rs.next()) {
        		  String table=getTableName(rs.getString(1),rs.getString(2));
        		  tables.add(table);
        		  Long id=rs.getLong(3);
        		  pidValue.put(id, rs.getString(4));
        		  strb.append(rs.getString(3));
	          }
         }catch (Exception e){
        	 e.printStackTrace();
         }
         return tables;
	}
	
	/**
     * Function: 	retain table set according to the attributes given
     * @param attributes 	attribute sets
     * @return 				Map<id，Parameter<tableName,attributeName>>
     */
	public Set<String> getTablesByAttributeIds(List<Integer> attributeIds, HashMap<Long,String> pidValue) throws SQLException{
		Set<String> tables=new HashSet<String>();
         try { 
        	 if(conn==null || conn.isClosed()){
        		 connect();
        	 }
        	  if(attributeIds==null){
        		  Statement statement = conn.createStatement();
    			  String sql = "select type, data_type, id, name from attribute_definition";
    			  ResultSet rs = statement.executeQuery(sql);
    			  while(rs.next()) {
    				  String table=getTableName(rs.getString(1),rs.getString(2));
    				  tables.add(table);
    				  Long id=rs.getLong(3);
    				  pidValue.put(id, rs.getString(4));
    			  }
    			  tables.remove("attribute_map");
    			  tables.remove("attribute_range");
        	  }
        	  else{
        		  for(int attribute:attributeIds){
        			  Statement statement = conn.createStatement();
        			  String sql = "select type, data_type, id, name from attribute_definition where id="+attribute+";";
        			  ResultSet rs = statement.executeQuery(sql);
        			  while(rs.next()) {
        				  String table=getTableName(rs.getString(1),rs.getString(2));
        				  tables.add(table);
        				  Long id=rs.getLong(3);
        				  pidValue.put(id, rs.getString(4));
        			  }
        		  }
        	  }
	    		  
         }catch (Exception e){
        	 e.printStackTrace();
         }
         return tables;
	}
	
	public Long getId(String name){
		try { 
			if(conn==null || conn.isClosed()){
       		 connect();
       	 	}
	        	  Statement statement = conn.createStatement();
	        	  String sql = "select id, name from attribute_definition where name='"+name+"';";
	        	  ResultSet rs = statement.executeQuery(sql);
	        	  while(rs.next()) {
	        		  Long id=rs.getLong(1);
	        		  return id;
		          }
	         }catch (Exception e){
	        	 e.printStackTrace();
	         }
		return null;
	}
	public static String getTableName(String t1, String t2){
		if(t1.equals("0")){	//literal
			switch(t2){
			case "1": case "3":	//integer
				return "attribute_integer";
			case "2":	//float
				return "attribute_float";
			case "4": case "41": case "42":	//time
				return "attribute_date_time";
			case "5":	//string
				return "attribute_String";
			case "6":	//range
				return "attribute_range";
			case "8":	//map
				return "attribute_map";
			default:
				return "";
			}
		}
		else{
			return "attribute_object";
		}
	}
	public static void close(){
		try {
			if(conn!=null && !conn.isClosed())
				conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
//	public static void main(String[] args) throws SQLException{
//		//Class.forName("org.logicalcobwebs.proxool.ProxoolDriver");
//		MySql con=new MySql();
//		List<String> attributes=new ArrayList<String>();
//		attributes.add("面积");
//		attributes.add("相克食物");
//		HashMap<Long,Parameter> tables=con.getTables(attributes);
//		System.out.println(tables);
//	}
}
