function submit(){
	var query=editor.getValue();
	if(query==null || query==""){
		alert("输入为空");
		return;
	}
	var result=document.getElementById("result");
	result.style.color="grey";
	result.innerHTML="处理中。。。";
	sparql.getResult(query, function(data){
		result.style.color="black";
		result.innerHTML=data;
	});
		
}

function init(){
	sparql.init();
	document.getElementById("result").innerHTML="查询结果此处显示";
}