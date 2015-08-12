package database;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBList;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import element.Filter;
import element.Order;
import element.Pair;

public class MyMongo {
	private static final String HOST = "mongodb://192.168.1.10:19130"; 
	private static int count=0;
    private static MongoDatabase database;
    private static MongoCollection<Document> collection;
    public MyMongo(){
    	MongoClientURI uri = new MongoClientURI(HOST,
    			MongoClientOptions.builder().cursorFinalizerEnabled(false));
    	MongoClient client = new MongoClient(uri);
    	database = client.getDatabase("ssco_zhoushan");
    }
    
    /**
     * Function:	query the MongoDB with a single attribute id on a single collection
     * @param tableName		the table that execute the query on
     * @param id			the attribute id in the query
     * @param pairs			the match and filter pattern
     * @param i				mark the difference of the column name
     * @return				query result
     */
    public List<Document> query(final String tableName, final HashMap<String,Pair> pairs, Set<String> cols){
    	count++;
    	System.out.println("query"+count+" "+tableName);
    	final List<Document> results=new ArrayList<Document>();
    	collection = database.getCollection(tableName);
    	Document match=new Document();
    	Document filter=new Document();
    	Document sort=new Document();
    	Document projection=new Document();
    	projection.append("_id", 0);
    	Iterator<Map.Entry<String,Pair>> iter = pairs.entrySet().iterator();
    	while (iter.hasNext()) {
			Map.Entry<String,Pair> entry = (Map.Entry<String, Pair>) iter.next();
			String colName=(String) entry.getKey();
			Pair pair=(Pair) entry.getValue();
			String value=pair.getValue();
			if(value!=null){
				match.append(colName, getBson(tableName,colName,value,false));
			}else{
				String alias=pair.getAlias();
				if(alias!=null && !alias.equals("")){
					projection.append(alias,"$"+colName);
					cols.add(alias);
				}
				List<Filter> filters=pair.getFilters();
				if(filters!=null && filters.size()>0){
					for(Filter f:filters){
						filter.append(colName, new Document("$"+f.getSymbol(),getBson(tableName,colName,f.getValue(), true)));
					}
				}
				Order order=pair.getOrder();
				if(order!=null){
					sort.append(colName, order.getIndex());
				}
			}
    	}
//		System.out.println(match);
//		System.out.println(filter);
//		System.out.println(sort);
//		System.out.println(projection);
    	
		List<Document> aggregates=new ArrayList<Document>();
							aggregates.add(new Document("$match",match));
							aggregates.add(new Document("$match",filter));
		if(sort.size()>0)	aggregates.add(new Document("$sort",sort));
							aggregates.add(new Document("$project",projection));
		
		AggregateIterable<Document> iterable = collection.aggregate(aggregates);
    	iterable.forEach(new Block<Document>() {
    	    @Override
    	    public void apply(final Document document) {
//    	    	System.out.println(document);
    	    	results.add(document);
    	    }
    	});
    	if(results.size()==0)return results;
    	String d;
    	for(Document document :results){
    		if(pairs.get("entity_id").getAlias()!=null){
	    		d=document.get(pairs.get("entity_id").getAlias()).toString();
	    		d+="#object";
	    		document.remove(pairs.get("entity_id").getAlias());
	    		document.put(pairs.get("entity_id").getAlias(),d);
    		}
        	if(tableName.equals("attribute_object") && pairs.get("attr_value").getAlias()!=null){
        		d=(String) document.get(pairs.get("attr_value").getAlias()).toString();
        		d+="#object";
        		document.remove(pairs.get("attr_value").getAlias());
        		document.put(pairs.get("attr_value").getAlias(),d);
        	}
    	}
    	return results;
    }
    
    private static BsonValue getBson(String tableName, String colName, String value, boolean isFilter){
    	try{
	    	if(colName=="entity_id"){
	        	if(isFilter){
	        		value=value.substring(value.lastIndexOf("/")+1,value.length()-1);
	        	}
	        	Long id=getId(value);
	        	if(id!=null)
	        		return new BsonInt64(id);
	        	else return null;
	    	}
	    	if(colName=="attr_value"){
		    	switch(tableName){
					case "attribute_String":return new BsonString(value);
					case "attribute_object":{
						if(isFilter){
			        		value=value.substring(value.lastIndexOf("/")+1,value.length()-1);
						}
						Long id=getId(value);
			        	if(id!=null)
			        		return new BsonInt64(id);
			        	else return null;
					}
					case "attribute_integer":return new BsonInt64(Long.parseLong(value));
					case "attribute_float":return new BsonDouble(Double.parseDouble(value));
					default:return null;
		    	}
		    }
	    	return new BsonInt64(Long.parseLong(value));

        }catch(NumberFormatException e){
        	return null;
        }
	}
    	
    /**
     * Function:	get attribute list (id) according to the entity name given
     * @param name
     * @return	entity's attribute id list
     */
    public List<Integer> getAttributes(String name){
    	final List<Long> ids=new ArrayList<Long>();
    	final List<Integer> attributes=new ArrayList<Integer>();
    	collection = database.getCollection("entity_id");
    	FindIterable<Document> iterable=collection.find(new Document("name",name));
    	iterable.forEach(new Block<Document>() {
    	    @Override
    	    public void apply(final Document document) {
    	    	ids.add((Long) document.get("id"));
    	    }
    	});
    	BasicDBList docIds = new BasicDBList();
    	docIds.addAll(ids);
    	collection = database.getCollection("attribute_summary");
    	iterable=collection.find(Filters.in("entity_id", docIds));
    	iterable.forEach(new Block<Document>() {
    	    @Override
    	    public void apply(final Document document) {
    	    	attributes.add((Integer) document.get("attr_id"));
    	    }
    	});
    	
    	return attributes;
    }
    public static Long getId(String name){
    	final List<Long> ids=new ArrayList<Long>();
    	MongoCollection<Document> collection = database.getCollection("entity_id");
    	FindIterable<Document> iterable=collection.find(new Document("name",name));
    	iterable.forEach(new Block<Document>() {
    	    @Override
    	    public void apply(final Document document) {
    	    	ids.add((Long) document.get("id"));
    	    }
    	});
    	if(ids.size()>0)
    		return ids.get(0);
    	else return null;
    }
    
    public static String getName(Long id){
    	final List<String> names=new ArrayList<String>();
    	MongoCollection<Document> collection = database.getCollection("entity_id");
    	FindIterable<Document> iterable=collection.find(new Document("id",id));
    	iterable.forEach(new Block<Document>() {
    	    @Override
    	    public void apply(final Document document) {
    	    	names.add((String) document.get("name"));
    	    }
    	});
    	if(names.size()>0)
    		return names.get(0);
    	else return null;
    }
    
    public static String getRandomName(){
    	Random random = new Random(); 
    	String result="";

    	for(int i=0;i<6;i++){
    		result+=random.nextInt(10);    
    	}
    	return result;
    }
   
    public void setRows(MongoCollection<Document> collection){
   		collection.drop();
   		List<Document> documents=new ArrayList<Document>();
   		Document document=null;
   		for(int j=0;j<100;j++){
   			System.out.println(j);
	    	for(int i=0;i<100000;i++){
//	    		document = new Document("_id", j*100000+i).append("value",(int)(Math.random()*100));
	    		document = new Document("_id", j*100000+i).append("name",getRandomName()).append("r1",(int)(Math.random()*10000000));
	    		documents.add(document);
	    	}
	    	Collections.shuffle(documents);
	    	System.out.println("generate complete");
	    	collection.insertMany(documents);
	    	documents.clear();
   		}
    }
    
    public void setDatabase(){
    	long startMili=System.currentTimeMillis();
    	final List<ObjectId> ids=new ArrayList<ObjectId>();
    	final MongoCollection<Document> collection1 = database.getCollection("attribute_summary");
    	FindIterable<Document> iterable=collection1.find();
    	iterable.filter(Filters.lt("attr_id", 4));
    	iterable.forEach(new Block<Document>() {
    	    @Override
    	    public void apply(final Document document) {
//    	    	System.out.println(document);
//    	    	if((Integer)document.get("attr_id")<4)
    	    	ids.add((ObjectId) document.get("_id"));
    	    }
    	});
//    	BasicDBList docIds = new BasicDBList();
//        docIds.addAll(ids);
//        System.out.println(docIds.size());
//    	FindIterable<Document> aiterable=collection2.find(Filters.in("r1", docIds));
//    	aiterable.forEach(new Block<Document>() {
//		    @Override
//		    public void apply(final Document document) {
//		    	results.add((Integer) document.get("_id"));
////		        System.out.println(document);
//		    }
//		});
////    	FindIterable<Document> iterable2=collection2.find();
////    	iterable2.forEach(new Block<Document>() {
////    	    @Override
////    	    public void apply(final Document document) {
////    	    	int a=(int) ((DBRef) document.get("r1")).getId();
////    	    	BsonValue value=new BsonInt32(a);
////    	    	FindIterable<Document> t=collection1.find(Filters.and(Filters.lt("value", 1),Filters.eq("_id",value)));
////    	    	if(t.first()!=null){
////        	    	System.out.println(document);
////    	    		results.add((Integer) document.get("_id"));
////    	    	}
////    	    }
////    	});
    	System.out.println(ids.size());

    	long endMili=System.currentTimeMillis();
    	long duration=endMili-startMili;
    	System.out.println((float)duration/1000+"秒");
//    	setRows(database.getCollection("R3"));
    	
    }
//    public static void main(String[] args){
//    	MyMongo mm=new MyMongo();
//    	List<Integer> attributes=mm.getAttributes("可可仙");
//    	System.out.println(attributes);
//    	
//    }
    
}

