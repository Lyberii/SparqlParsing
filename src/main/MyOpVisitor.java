package main;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.bson.Document;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.algebra.OpVisitor;
import com.hp.hpl.jena.sparql.algebra.op.OpAssign;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpConditional;
import com.hp.hpl.jena.sparql.algebra.op.OpDatasetNames;
import com.hp.hpl.jena.sparql.algebra.op.OpDiff;
import com.hp.hpl.jena.sparql.algebra.op.OpDisjunction;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.algebra.op.OpExtend;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpGroup;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLabel;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpList;
import com.hp.hpl.jena.sparql.algebra.op.OpMinus;
import com.hp.hpl.jena.sparql.algebra.op.OpNull;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpPath;
import com.hp.hpl.jena.sparql.algebra.op.OpProcedure;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpPropFunc;
import com.hp.hpl.jena.sparql.algebra.op.OpQuad;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadBlock;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;
import com.hp.hpl.jena.sparql.algebra.op.OpSequence;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpTable;
import com.hp.hpl.jena.sparql.algebra.op.OpTopN;
import com.hp.hpl.jena.sparql.algebra.op.OpTriple;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;

import database.MyMongo;
import database.MySql;
import element.Extend;
import element.Filter;
import element.Group;
import element.Order;
import element.Pair;
import element.Triple;

public class MyOpVisitor implements OpVisitor{

	private MySql mysql;
	private MyMongo mymongo;
	private List<String> cols;
	public HashMap<Long,String> pidValue;
	public Set<String> ps;
	
	List<Triple> triples;
	Stack<List<Document>> qResultss;
	List<Document> qResults;
	List<Filter> filters;
	HashMap<String,String> eMap;
	Order order;
	Group group;
	
	public MyOpVisitor(){
		mysql=new MySql();
		mymongo=new MyMongo();
		qResultss=new Stack<List<Document>>();
		pidValue=new HashMap<Long,String>();
		ps=new HashSet<String>();
		cols=new ArrayList<String>();
		cols.add("entity_id");
		cols.add("attr_id");
		cols.add("attr_value");
	}
	
	@Override
	public void visit(OpBGP arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		List<Triple> triples=new ArrayList<Triple>();
		for(int i=0;i<arg0.getPattern().size();i++){
			Triple triple=new Triple();
			Node subject=arg0.getPattern().get(i).getSubject();
			Node predicate=arg0.getPattern().get(i).getPredicate();
			Node object=arg0.getPattern().get(i).getObject();
			String ts=getType(subject);
			String tp=getType(predicate);
			String to=getType(object);
			String vs=getValue(subject);
			String vp=getValue(predicate);
			String vo=getValue(object);
			Set<String> tables=null;
			List<Integer> attributeIds=null;
			if(!tp.equals("var")){
				String[] us=predicate.getURI().split("/");
				String domain=us[us.length-2];
				try {
					StringBuffer strb=new StringBuffer();
					tables=mysql.getTableByAttributeName(vp,domain,pidValue,strb);
					vp=strb.toString();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			else{
				ps.add(vp);
			}
			if(!to.equals("var") && tables==null){
				String table="attribute_"+to;
				tables=new HashSet<String>();
				tables.add(table);
				
			}

			if(!ts.equals("var")){
				attributeIds=mymongo.getAttributes(vs);
				if(tables==null){
					try {
						tables=mysql.getTablesByAttributeIds(attributeIds,pidValue);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
			if(tables==null){
				try {
					tables=mysql.getTablesByAttributeIds(null, pidValue);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			List<String> pos=new ArrayList<String>();
			pos.add(vs);
			pos.add(vp);
			pos.add(vo);
			List<String> posType=new ArrayList<String>();
			posType.add(ts);
			posType.add(tp);
			posType.add(to);
			triple.setPos(pos);
			triple.setPosType(posType);
			triple.setTables(tables);
			triples.add(triple);
		}
		if(this.triples!=null){
			dealBGP();
		}
		this.triples=triples;
	}
	
	private static String getValue(Node node){
		String value="";
		if(node instanceof com.hp.hpl.jena.sparql.core.Var){
			value=node.getName();
		}
		else if(node instanceof com.hp.hpl.jena.graph.Node_URI){
			if(node.toString().contains("#"))
				value=node.toString().split("#")[1];
			else{
				String[] t=node.toString().split("/");
				value=t[t.length-1];
			}
		}
		else if(node instanceof com.hp.hpl.jena.graph.Node_Literal){
			value=node.getLiteralValue().toString();
		}
		return value;
	}
	
	private static String getType(Node node){
		String type="";
		if(node instanceof com.hp.hpl.jena.sparql.core.Var){
			type="var";
		}
		else if(node instanceof com.hp.hpl.jena.graph.Node_URI){
			type="object";
		}
		else if(node instanceof com.hp.hpl.jena.graph.Node_Literal){
			String t=node.toString();
			if(t.startsWith("\"")){
				if(t.contains("\"^^")){
					String[] tmps=node.getLiteralDatatypeURI().split("#");
					type=tmps[tmps.length-1];
				}
				else
					type="String";
			}
			else{
				if(t.contains(".")){
					type="float";
				}
				else{
					type="integer";
				}
			}
		}
		else{
		}
		return type.equals("decimal")?"float":type;
	}
	
	@Override
	public void visit(OpQuadPattern arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpQuadBlock arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpTriple arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpQuad arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpPath arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpTable arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpNull arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpProcedure arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpPropFunc arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpFilter arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
//		System.out.println(arg0.getExprs().get(0).getFunction().getArg(1).getVarName());
		List<Filter> filters=new ArrayList<Filter>();
		for(int i=0;i<arg0.getExprs().size();i++){
			Filter f=new Filter();
			String name="";
			String value="";
			Expr var1=arg0.getExprs().get(i).getFunction().getArg(1);
			Expr var2=arg0.getExprs().get(i).getFunction().getArg(2);
			String symbol=arg0.getExprs().get(i).getFunction().getFunctionSymbol().getSymbol();
			if(var1.toString().startsWith("?") && !var2.toString().startsWith("?")){
				name=var1.getVarName();
				value=var2.toString();
				symbol=getSymbol(symbol,false);
			}
			else if(!var1.toString().startsWith("?") && var2.toString().startsWith("?")){
				name=var2.getVarName();
				value=var1.toString();
				symbol=getSymbol(symbol,true);
			}
			f.setVar(name);
			f.setSymbol(symbol);
			f.setValue(value);
			filters.add(f);
			
		}
//		System.out.println(exprs);
		
		if(triples==null){
			qResults=qResultss.pop();
			filter(qResults,filters);
			this.filters=null;
			qResultss.push(qResults);
		}else{
			this.filters=filters;
		}
	}
	
	/**
	 * Function:	get rid of the expression difference of mongo and sparql language
	 * @param symbol
	 * @param reverse	reverse the symbol if true
	 * @return
	 */
	private String getSymbol(String symbol,boolean reverse){
		switch (symbol){
			case "gt": return reverse?"lt":"gt";
			case "lt": return reverse?"gt":"lt";
			case "ge":return reverse?"lte":"gte";
			case "le":return reverse?"gte":"lte";
			default: return symbol;
		}
	}
	
	@Override
	public void visit(OpGraph arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpService arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpDatasetNames arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpLabel arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpAssign arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpExtend arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		HashMap<String,String> eMap=new HashMap<String,String>();
		Map<Var,Expr> aMap=arg0.getVarExprList().getExprs();
		Iterator<Entry<Var,Expr>> iter = aMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<Var,Expr> entry = (Map.Entry<Var,Expr>) iter.next();
			Var key = entry.getKey();
			Expr val = entry.getValue();
			eMap.put(val.getVarName(),key.getVarName());
		}
		
		List<String> varTypes=new ArrayList<String>();
		if(triples!=null){
			dealBGP();

		}
		qResults=qResultss.pop();
		qResults=group(qResults,group,eMap,varTypes);
		groupOrder(qResults,group.getNames(),varTypes);
		qResultss.push(qResults);
		group=null;
	}

	@Override
	public void visit(OpJoin arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		while(qResultss.size()<2){
			dealBGP();
		}
		List<Document> left=qResultss.pop();
		List<Document> right=qResultss.pop(); 
		List<Iterator<Document>> toJoin=new ArrayList<Iterator<Document>>();
		toJoin.add(left.iterator());
		toJoin.add(right.iterator());
		qResults=join(toJoin);
		qResultss.push(qResults);
		
		
	}

	@Override
	public void visit(OpLeftJoin arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpUnion arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpDiff arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpMinus arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpConditional arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpSequence arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpDisjunction arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpExt arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpList arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpOrder arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
//		System.out.println(arg0.getConditions().get(0).getDirection());
		int direction=arg0.getConditions().get(0).getDirection();
		Order order=null;
		switch(direction){
			case -1:order=Order.DESC;break;
			case 1:
			default:order=Order.ASC;break;
		}
		order.setName(arg0.getConditions().get(0).getExpression().getVarName());
		
		if(triples!=null){
			this.order=order;
		}
		else{
			qResults=qResultss.pop();
			order(qResults,order);
			qResultss.push(qResults);
			this.order=null;
		}
	}

	@Override
	public void visit(OpProject arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		List<String> projections=new ArrayList<String>();
		List<Var> args=arg0.getVars();
		for(Var arg:args){
			projections.add(arg.getName());
		}

		if(triples!=null){
			dealBGP();
		}
		List<Document> qResults=qResultss.pop();
		project(qResults,projections);
		qResultss.push(qResults);
	}

	@Override
	public void visit(OpReduced arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpDistinct arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}

	@Override
	public void visit(OpSlice arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
//		System.out.println(arg0.getLength());
		long[] slices=new long[2];
		slices[0]=arg0.getStart()<=0?-1:arg0.getStart();
		slices[1]=arg0.getLength();

		if(triples!=null){
			dealBGP();
		}
		qResults=qResultss.pop();
		slice(qResults, slices);
		qResultss.push(qResults);
	}

	@Override
	public void visit(OpGroup arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		List<Var> t=arg0.getGroupVars().getVars();
		List<String> groupNames=new ArrayList<String>();
		Extend e;
		List<Extend> extend=new ArrayList<Extend>();
		for(Var tn:t){
			groupNames.add(tn.getName());
		}
		List<ExprAggregator> eas=arg0.getAggregators();
		for(int i=0;i<eas.size();i++){
			e=new Extend();
			e.setName(eas.get(i).getAggregator().getExprList().get(0).getVarName());
			e.setFunction(eas.get(i).getAggregator().getName());
			e.setPname(eas.get(i).getAggVar().getVarName());
			extend.add(e);
		}
		Group group=new Group();
		group.setNames(groupNames);
		group.setAggregates(extend);
		
		this.group=group;

	}

	@Override
	public void visit(OpTopN arg0) {
		// TODO Auto-generated method stub
		System.out.println(arg0.getClass());
		
	}
	
	private void documentMap(List<DocumentMap> dms){
		if(dms.size()==1)return;
		boolean modified=true;
		int size=dms.size();
		int i,j;
		while(modified){
			modified=false;
			for(i=0;i<size;i++){
				for(j=i+1;j<size;j++){
					if(!dms.get(j).equals(dms.get(i))){
						Set<String> dmcol=dms.get(i).getCols();
						Set<String> dcol=dms.get(j).getCols();
						Set<String> dups=getSame(dmcol,dcol);
						if(dups.size()!=0){
							dmcol.addAll(dms.get(j).getCols());
							dms.get(i).getDocus().addAll(dms.get(j).getDocus());
							modified=true;
							dms.remove(dms.get(j));
							size--;
							break;
						}
					}
				}
			}
		}
	}
	
	private List<Document> join(List<Iterator<Document>> documents){
		Set<String> cols=new HashSet<String>();
		List<Document> docus=new ArrayList<Document>();
		if(documents.size()==1){
			Iterator<Document> it=documents.get(0);
			while(it.hasNext()){
				Document document=it.next();
				docus.add(document);
			}
			return docus;
		}
		Set<String> dups;
		for(int i=0;i<documents.size();i++){
			Iterator<Document> it=documents.get(i);
//			List<String> col=new ArrayList<String>();
			Set<String> col=null;
			List<Document> mid=new ArrayList<Document>();
			boolean flag=false;
			boolean reset=true;
			if(!it.hasNext()){
				return new ArrayList<Document>();
			}
			while(it.hasNext()){
				Document document=it.next();
				if(!flag){
					col = new HashSet<String>(document.keySet());
					flag=true;
				}
				dups=getSame(col,cols);
				if(dups.size()==0){
					reset=false;
					if(docus.size()==0){
						mid.add(document);
					}
					else{
						for(Document docu:docus){
							mid.add(merge(document,docu));
						}
					}
				}
				else{
					boolean match=false;
					for(Document docu:docus){
						boolean m=true;
						for(String dup:dups){
							if(!docu.get(dup).equals(document.get(dup))){
								m=false;
								break;
							}
						}
						if(m){
							mid.add(merge(document,docu));
							match=true;
						}
					}
					if(match){
						reset=false;
					}
				}
			}
			if(reset){
				docus.clear();
				cols.clear();
			}else{
				cols.addAll(col);
				docus=mid;
			}
		}
		return docus;
	}
	
	private void filter(List<Document> documents, List<Filter> filters){
		if(filters==null || filters.size()==0)return;
		int size=documents.size();
		for(int i=0;i<size;i++){
			boolean match=true;
			for(Filter filter:filters){
				Object o=documents.get(i).get(filter.getVar());
				if(o!=null){
					if(!match(o,filter)){
						match=false;
						break;
					}
				}else match=false;
			}
			if(!match){
				documents.remove(i);
				i--;
				size--;
			}
		}
		return;
	}
	private boolean match(Object value,Filter filter){
		String type=value.getClass().getSimpleName();
		if(type!="String"){
			Double v=Double.valueOf(value.toString());
			Double f=Double.valueOf(filter.getValue());
			switch(filter.getSymbol()){
			case "lt":return v<f;
			case "gt":return v>f;
			case "lte":return v<=f;
			case "gte":return v>=f;
			default:return false;
			}
		}else{
			
		}
		return false;
	}
	private void order(List<Document> documents,Order order){
		if(documents.size()==0) return;
		List<Document> results=new ArrayList<Document>();
		String name=order.getName();
		String type=documents.get(0).get(name).getClass().getSimpleName();
		Document temp=null;
		while(documents.size()!=0){
			for(Document document:documents){
				if(temp==null){
					temp=document;
				}
				else{
					switch(type){
						case "Long":{
							if(order==Order.ASC){
								if(document.getLong(name)<temp.getLong(name)){
									temp=document;
								}
							}
							else if(document.getLong(name)>temp.getLong(name)){
								temp=document;
							}
							break;
						}
						case "Double":{
							if(order==Order.ASC){
								if(document.getDouble(name)<temp.getDouble(name)){
									temp=document;
								}
							}
							else if(document.getDouble(name)>temp.getDouble(name)){
								temp=document;
							}
							break;
						}
						case "String":{
							if(order==Order.ASC){
								if(document.getString(name).compareTo(temp.getString(name))<0){
									temp=document;
								}
							}
							else if(document.getString(name).compareTo(temp.getString(name))>0){
								temp=document;
							}
							break;
						}
						default:break;
					}
				}
			}
			documents.remove(temp);
			results.add(temp);
			temp=null;
		}
		documents.addAll(results);
	}
	
	private void groupOrder(List<Document> documents, List<String> names, List<String> varTypes){
		List<Document> results=new ArrayList<Document>();
		Document temp=null;
		while(documents.size()!=0){
			for(Document document:documents){
				if(temp==null){
					temp=document;
				}
				else{
					boolean got=false;
					for(int i=0;i<names.size();i++){
						String name=names.get(i);
						switch(varTypes.get(i)){
							case "Long":{
								if(document.getLong(name)<temp.getLong(name)){
									temp=document;
									got=true;
								}
								break;
							}
							case "Double":{
								if(document.getDouble(name)<temp.getDouble(name)){
									temp=document;
									got=true;
								}
								break;
							}
							case "String":{
								if(document.getString(name).compareTo(temp.getString(name))<0){
									temp=document;
									got=true;
								}
								break;
							}
							default:break;
						}
						if(got)break;
					}
				}
			}
			documents.remove(temp);
			results.add(temp);
			temp=null;
		}
		documents.addAll(results);
	}
	
	private List<Document> group(List<Document> documents, Group group, HashMap<String,String> eMap,List<String> varTypes){
		if(group==null)return documents;
		List<String> groupNames=group.getNames();
		boolean first=true;
		List<Object> keys=null;
		boolean match;
		HashMap<List<Object>,List<Document>> map=new HashMap<List<Object>,List<Document>>();
		for(Document document:documents){
			keys=new ArrayList<Object>();
			match=true;
			for(String name:groupNames){
				Object var=document.get(name);
				if(var!=null){
					if(first)
						varTypes.add(var.getClass().getSimpleName());
					keys.add(var);
				}
				else{
					match=false;
					break;
				}
			}
			first=false;
			if(match){
				List<Document> docus;
				if(map.get(keys)==null){
					docus=new ArrayList<Document>();
					docus.add(document);
					map.put(keys, docus);
				}
				else{
					docus=map.get(keys);
					docus.add(document);
				}
			}
		}
		List<Extend> extend=group.getAggregates();
		List<Document> results=new ArrayList<Document>();
		Iterator<Entry<List<Object>, List<Document>>> iter = map.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<List<Object>,List<Document>> entry = (Map.Entry<List<Object>,List<Document>>) iter.next();
			keys = entry.getKey();
			documents = entry.getValue();
			long l=0,count=0;
			double d=0;
			Document result=new Document();
			for(int i=0;i<groupNames.size();i++){
				result.append(groupNames.get(i),keys.get(i));
			}
			for(Extend e:extend){
				String type="";
				String var=e.getName();
				String pro=eMap.get(e.getPname());
				switch(e.getFunction()){
					case "COUNT":{
						for(Document document:documents){
							if(document.get(var)!=null){
								type="LONG";
								l++;
							}
						}
						break;
					}
					case "SUM":{
						for(Document document:documents){
							Object t=document.get(var);
							if(t!=null){
								if(t instanceof java.lang.Long){
									type="LONG";
									l+=(long)t;
								}
								else if(t instanceof java.lang.Double){
									type="DOUBLE";
									d+=(double)t;
								}
							}
						}
						break;
					}
					case "AVG":{
						for(Document document:documents){
							Object t=document.get(var);
							if(t!=null){
								count++;
								if(t instanceof java.lang.Long){
									type="LONG";
									l+=(long)t;
								}
								else if(t instanceof java.lang.Double){
									type="DOUBLE";
									d+=(double)t;
								}
							}
						}
						switch(type){
							case "LONG":d=l/count;break;
							case "DOUBLE":d=d/count;break;
							default:break;
						}
						type="DOUBLE";
						break;
					}
					default:break;
				}
				switch(type){
					case "LONG":result.append(pro, l);break;
					case "DOUBLE":result.append(pro, d);break;
				default:break;
				}
			}
			results.add(result);
		}
		return results;
	}

	private void slice(List<Document> documents, long[] slices){
		if(documents.size()==0)return;
		if(slices!=null){
			if(slices[1]<0){
				documents.subList(0,(int) slices[0]).clear();
			}
			else{
				int start=slices[0]<0?0:(int) slices[0];
				int end=(int) slices[1];
				documents.subList(0,start).clear();
				if(end<documents.size())
					documents.subList((int) (slices[1]),documents.size()).clear();
			}
		}
	}
	
	private List<Document> cartesian(List<List<Document>> mids){
		if(mids.size()==1)return mids.get(0);
		List<Document> results=new ArrayList<Document>();
		List<Document> tmp=new ArrayList<Document>();
		for(List<Document> mid:mids){
			if(results.size()==0){
				results.addAll(mid);
				continue;
			}
			for(Document m:mid){
				for(Document r:results){
					tmp.add(merge(m,r));
				}
			}
			results=tmp;
		}
		return results;
	}
	
	
	
	/**
	 * Function:	project the querying result according to the projection list
	 * @param documents 	documents to project
	 * @param projections 	column list to project
	 * @return projection 	results
	 */
	private void project(List<Document> documents, List<String> projections){
		if(documents.size()==0)return;
		List<String> removes=new ArrayList<String>();
		Document d1=documents.get(0);
		Iterator<Map.Entry<String,Object>> d=d1.entrySet().iterator();
		while (d.hasNext()) { 
			Map.Entry<String,Object> entry = (Map.Entry<String,Object>) d.next(); 
		    String key = (String)entry.getKey(); 
		    if(!projections.contains(key))removes.add(key);
		} 
		for(Document document:documents){
			for(String remove:removes)document.remove(remove);
		}
		
	}
	
	public static Set<String> getSame(Set<String> col,Set<String> cols){
		Set<String> result=new HashSet<String>();
		for(String c:col){
			if(cols.contains(c)){
				result.add(c);
			}
		}
		return result;
	}
	
	/**
	 * Function:	merge the documents (without analyzing if should to)
	 * @param document1
	 * @param document2
	 * @return 		merge result
	 */
	public static Document merge(Document document1, Document document2){
		Document t=new Document();
		Iterator<Map.Entry<String,Object>> di=document1.entrySet().iterator();
		while (di.hasNext()) { 
		    Map.Entry<String,Object> entry = (Map.Entry<String,Object>) di.next(); 
		    String key = (String)entry.getKey(); 
		    Object val = entry.getValue(); 
		    t.append(key, val);
		} 
		di=document2.entrySet().iterator();
		while (di.hasNext()) { 
		    Map.Entry<String,Object> entry = (Map.Entry<String,Object>) di.next(); 
		    String key = (String)entry.getKey(); 
		    Object val = entry.getValue(); 
		    t.append(key, val);
		} 
		return t;
	}
	public List<Document> getQResults(){
		return qResultss.pop();
	}
	
	private void dealBGP(){
		Pair pair;
		List<DocumentMap> dms=new ArrayList<DocumentMap>();
		for(Triple triple : triples){
			Set<String> columns=new HashSet<String>();
			List<Document> oneTriple=new ArrayList<Document>();
			List<List<Document>> oneOrder=new ArrayList<List<Document>>();
			HashMap<String,Pair> pairs=new HashMap<String,Pair>();
			for(int i=0;i<triple.getPos().size();i++){
				pair=new Pair();
				if(!triple.getPosType().get(i).equals("var")){
					pair.setValue(triple.getPos().get(i));
				}
				else{
					pair.setAlias(triple.getPos().get(i));
					if(order!=null && triple.getPos().get(i).equals(order.getName()))pair.setOrder(order);
					if(filters!=null){
						List<Filter> fs=new ArrayList<Filter>();
						for(Filter filter:filters){
							if(triple.getPos().get(i).equals(filter.getVar())){
								fs.add(filter);
							}
						}
						pair.setFilters(fs);
					}
				}
				pairs.put(cols.get(i),pair);
			}
			Set<String> tables=triple.getTables();
			for(String table:tables){
				List<Document> result=new ArrayList<Document>();
				if(triple.getPosType().get(0)=="object" || triple.getPosType().get(0)=="var"){
					result=mymongo.query(table,pairs,columns);
				}
				if(order!=null && tables.size()>1 && result.size()>0)
					oneOrder.add(result);
				oneTriple.addAll(result);
				
			}
			if(oneOrder.size()>0){
				oneTriple.clear();
				oneTriple.addAll(getOrder(oneOrder,order));
			}
			Iterator<Document> results=oneTriple.iterator();
			dms.add(new DocumentMap(columns, results));
		}
		documentMap(dms);
		List<List<Document>> mapResult=new ArrayList<List<Document>>();

		for(DocumentMap dm:dms){
			mapResult.add(join(dm.getDocus()));
		}
		qResults=cartesian(mapResult);
		qResultss.push(qResults);
		order=null;
		filters=null;
		triples=null;
	}
	
	private List<Document> getOrder(List<List<Document>> qResults, Order order){
		List<Document> results=new ArrayList<Document>();
		for(List<Document> rs:qResults){
			results=oMerge(results,rs,order);
		}
		return results;
	}
	
	private List<Document> oMerge(List<Document> d1s, List<Document> d2s, Order order){
		String name=order.getName();
		if(d1s.size()==0)return d2s;
		List<Document> results=new ArrayList<Document>();
		String type1=d1s.get(0).get(name).getClass().getSimpleName();
		String type2=d1s.get(0).get(name).getClass().getSimpleName();
		if(!type1.equals(type2)){
			results.addAll(d1s);
			results.addAll(d2s);
			return results;
		}
		String type=type1;
		int i=0,j=0;
		Document d1=null,d2=null;
		while(i!=d1s.size() && j!=d2s.size()){
			if(i==d1s.size()){
				results.addAll(d2s.subList(j, d2s.size()));
				j=d2s.size();
				break;
			}
			if(j==d2s.size()){
				results.addAll(d1s.subList(i, d1s.size()));
				i=d1s.size();
				break;
			}
			d1=d1s.get(i);
			d2=d2s.get(j);
			switch(type){
				case "Long":{
					if(order==Order.ASC){
						if(d1.getLong(name)<d2.getLong(name)){
							results.add(d1);
							i++;
						}
						else{
							results.add(d2);
							j++;
						}
					}
					else{
						if(d1.getLong(name)>d2.getLong(name)){
							results.add(d1);
							i++;
						}
						else{
							results.add(d2);
							j++;
						}
					}
					break;
				}
				case "Double":{
					if(order==Order.ASC){
						if(d1.getDouble(name)<d2.getDouble(name)){
							results.add(d1);
							i++;
						}
						else{
							results.add(d2);
							j++;
						}
					}
					else{
						if(d1.getDouble(name)>d2.getDouble(name)){
							results.add(d1);
							i++;
						}
						else{
							results.add(d2);
							j++;
						}
					}
					break;
				}
				case "String":{
					if(order==Order.ASC){
						if(d1.getString(name).compareTo(d2.getString(name))<0){
							results.add(d1);
							i++;
						}
						else{
							results.add(d2);
							j++;
						}
					}
					else{
						if(d1.getString(name).compareTo(d2.getString(name))>0){
							results.add(d1);
							i++;
						}
						else{
							results.add(d2);
							j++;
						}
					}
					break;
				}
				default:break;
			}
		}
		return results;
	}
}

class DocumentMap {
	Set<String> cols;
	List<Iterator<Document>> docus;

	public DocumentMap(Set<String> cols2, Iterator<Document> docus) {
		this.cols = cols2;
		this.docus = new ArrayList<Iterator<Document>>();
		this.docus.add(docus);
	}
	public Set<String> getCols() {
		return cols;
	}
	public void setCols(Set<String> cols) {
		this.cols = cols;
	}
	public List<Iterator<Document>> getDocus() {
		return docus;
	}
	public void setDocus(List<Iterator<Document>> docus) {
		this.docus = docus;
	}
}
