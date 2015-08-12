package main;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QueryParseException;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;

import database.MyMongo;
import database.MySql;

public class Sparql {
	private static MyOpVisitor mov;

	public static void init(){
		MySql.close();
		mov=new MyOpVisitor();
	}
	
	public static void main(String[] args){
		String select="PREFIX  island:  <http://hiekn.com/10046894/> " +
    		"SELECT  (COUNT(?x) AS ?count) ?p " +
    		"WHERE   { ?x island:所属区域 ?p ." +
    		"FILTER (400 > ?altitude)" +
    		"?x island:海拔 ?altitude }" +
    		"GROUP BY ?p "+
    		"HAVING (?count > 40) "+
    		"ORDER BY desc(?count) " +
    		"LIMIT 4 " +
    		"OFFSET 1";
//		String select1="PREFIX  island:  <http://hiekn.com/10046894/> " +
//	    		"SELECT  ?p ?o  " +
//	    		"WHERE   {island:舟山岛 ?p ?o}";
		init();
		System.out.println(getResult(select));
		MySql.close();
	}
	
	public static String getResult(String select){
		Op op=null;
		try{
			Query query = QueryFactory.create(select);
			op = Algebra.compile(query);
			System.out.println(op);
		}catch(QueryParseException e){
			e.printStackTrace();
			return "Sparql语法错误!";
		}
		
		
    	long startMili=System.currentTimeMillis();
    	
		OpWalker.walk(op, mov);
		List<Document> results=mov.getQResults();
		System.out.println("Query Result:\t"+results.size()+"\titem(s)");
//		System.out.println(results);
    	long endMili=System.currentTimeMillis();
    	long duration=endMili-startMili;
    	System.out.println((float)duration/1000+"秒");
    	String result=formatOutput(results);
		return result;
	}
	
	public static String formatOutput(List<Document> results){
		String head="";
		String content="";
		int size=results.size();
		if(size==0)
			return "No results";
		else{
			int gap=size-30;
			if(gap>0){
				size=30;
			}
			for(int i=0;i<size;i++){
				Document d=results.get(i);
				Iterator<Map.Entry<String,Object>> di=d.entrySet().iterator();
				while (di.hasNext()) { 
				    Map.Entry<String,Object> entry = (Map.Entry<String,Object>) di.next(); 
				    String key = (String)entry.getKey(); 
				    String val = ((Object)entry.getValue()).toString(); 
				    if(mov.ps.contains(key)){
				    	val=mov.pidValue.get(Long.valueOf(val));
				    }
				    else{
					    if(val.contains("#")){
					    	val=MyMongo.getName(Long.valueOf(val.split("#")[0]));
					    }
				    }
				    if(i==0)
				    	head+=key+"\t";
				    content+=val+"\t";
				} 
				content+="\n";
			}
			if(gap>0){
				content+="\n"+gap+" more items...";
			}
			return head+"\n\n"+content;
		}
	}
	
}
