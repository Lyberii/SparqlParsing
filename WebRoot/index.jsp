<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
  <head>
    
    <title>Sparql</title>
	<meta http-equiv="pragma" content="no-cache">
	<meta http-equiv="cache-control" content="no-cache">
	<meta http-equiv="expires" content="0">    
	
	
    <script type='text/javascript' src='dwr/interface/sparql.js'></script>  
    <script type='text/javascript' src='dwr/engine.js'></script>  
    <script type='text/javascript' src='dwr/util.js'></script>  
	<script type='text/javascript' src="js/index.js"></script>
	<script type='text/javascript' src="js/codemirror.js"></script>
	<script type='text/javascript' src="js/sparql.js"></script>
	<script type='text/javascript' src="js/matchbrackets.js"></script>
	
	<link rel="stylesheet" type="text/css" href="css/index.css">
	<link rel="stylesheet" type="text/css" href="css/codemirror.css">
  </head>
  
  <body onload="init();">
    <div></div>
    <div id="content">
	    <div id="up">
		    <div id="input">
				<textarea id="query">
PREFIX  island:  &lt;http://hiekn.com/10046894/&gt; 
SELECT  (COUNT(?x) AS ?count) ?p 
WHERE   { ?x island:所属区域 ?p . 
	FILTER (400 &gt; ?altitude) 
	?x island:海拔 ?altitude } 
GROUP BY ?p 
HAVING (?count &gt; 40) 
ORDER BY desc(?count) 
LIMIT 4 
OFFSET 1 </textarea>
				
				<script>
			      editor = CodeMirror.fromTextArea(document.getElementById("query"), {
			        mode: "application/sparql-query",
        			lineNumbers: true,
       	 			matchBrackets: true
			      });
			    </script>
			<div id="button"><button onclick="submit();">查询</button></div>
		    </div>
	    </div>
	    <div id="down">
	    	<textarea id="result" readonly="readonly"></textarea>
	    </div>
    
    </div>
    <div></div>
  </body>
</html>
