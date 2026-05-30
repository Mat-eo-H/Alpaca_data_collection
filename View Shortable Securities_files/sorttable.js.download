//Sortable table script
//Author: Allan Betschart
var prevIndex=-1;
var sortReset=false;
function SortTable(obj,type,columnIndex,targetTable)
{
  if (targetTable != null)
    {prevIndex=targetTable.getAttribute("prevIndex");}
  if (columnIndex == null)
    {columnIndex = obj.cellIndex;}
  if(obj == null)
    {return;}
  var children = obj.parentNode.childNodes;
  for (var c = 0; c < children.length; c++)
  { 
    if (children[c].nodeName == "#text")
      {continue;}
    if (children[c].id == obj.id)
      {children[c].className="menuHover";}
    else
      {children[c].className="menuUp";}
  }
  var cellStyle=new Array("CellOff","CellOn");
  var flipflop=0;
  if (type == null)
    {type = "";}
  var table = (targetTable != null) ? targetTable : obj.parentNode.parentNode.parentNode;
  var body = table.getElementsByTagName("tbody")[0];
  if (window.Node == null)
  {
    if (table.childNodes.length > 1)
      {body=table.childNodes[2];}
    else
      {body=table.childNodes[0];}
  }
  var rows = body.getElementsByTagName("tr");
  var origRows = new Array();
  for (var i=0; i< rows.length; i++)
  {
     origRows[i] = new Object;
     origRows[i].index = i;
     if (rows[i].getElementsByTagName("td")[columnIndex] == null)
       {return;}
     if (rows[i].getElementsByTagName("td")[columnIndex].hasChildNodes())
     {
       if(rows[i].getElementsByTagName("td")[columnIndex].childNodes[0].childNodes.length == 0)
       {
         var list=rows[i].getElementsByTagName("td")[columnIndex].childNodes;
         if (list.length == 1)
         {
           var node=list[0];
           if (/img/i.test(node.nodeName))
             {origRows[i].value = node.alt;}
           else if (/label/i.test(node.nodeName))
             {origRows[i].value = node.title;}
           else
             {origRows[i].value = node.nodeValue;}
         }
         else
            {origRows[i].value=rows[i].getElementsByTagName("td")[columnIndex].innerHTML.replace(/<br\/?>/i," ");}
       }
       else
       {
         var node=rows[i].getElementsByTagName("td")[columnIndex].childNodes[0];
         if (/label/i.test(node.nodeName))
           {origRows[i].value = node.title;}
         else
           {origRows[i].value = node.childNodes[0].nodeValue;}
       }
     }
     else
       {origRows[i].value = "";}
  }
  if((columnIndex == prevIndex)&&(!sortReset))
    {origRows.reverse();}
  else
  {
     sortReset=false;
     switch(type.toUpperCase())
     {
       case 'STRING': origRows.sort(CompareCase); break;
       case 'INSENSITIVE': origRows.sort(Compare); break;
       case 'NUMBER': origRows.sort(CompareDigits); break;
       case 'DATE': origRows.sort(CompareDates); break;
       default: origRows.sort(Compare); break;
     }
     if ((prevIndex < 0)&&(columnIndex == 0))
       {origRows.reverse();}
     prevIndex=columnIndex;
     if (targetTable != null)
       {targetTable.setAttribute("prevIndex",prevIndex);}
  }
  var sortedBody = document.createElement("tbody");
  for (var i=0; i<origRows.length; i++)
  {
    sortedBody.appendChild(rows[origRows[i].index].cloneNode(true));
    if (rows[origRows[i].index].onclick)
      {sortedBody.lastChild.onclick=rows[origRows[i].index].onclick;}
    var sTD=sortedBody.lastChild.getElementsByTagName('td');
    var oTD=rows[origRows[i].index].getElementsByTagName('td');
    for (var itd=0; itd< sTD.length; itd++)
      {sTD[itd].onclick=oTD[itd].onclick;}
    var sImg=sortedBody.lastChild.getElementsByTagName('img');
    var oImg=rows[origRows[i].index].getElementsByTagName('img');
    for (var ims=0; ims< sImg.length; ims++)
    {
      sImg[ims].onclick=oImg[ims].onclick;
      sImg[ims].className=oImg[ims].className;
      sImg[ims].onmouseover=oImg[ims].onmouseover;
    }
    var sA=sortedBody.lastChild.getElementsByTagName('a');
    var oA=rows[origRows[i].index].getElementsByTagName('a');
    for (var ias=0; ias< sA.length; ias++)
    {
      sA[ias].onclick=oA[ias].onclick;
      sA[ias].className=oA[ias].className;
      sA[ias].onmouseover=oA[ias].onmouseover;
    }
    var sF=sortedBody.lastChild.getElementsByTagName('font');
    var oF=rows[origRows[i].index].getElementsByTagName('font');
    for (var ifs=0; ifs< sF.length; ifs++)
    {
      sF[ifs].onclick=oF[ifs].onclick;
      sF[ifs].className=oF[ifs].className;
      sF[ifs].onmouseover=oF[ifs].onmouseover;
    }
    var sIn=sortedBody.lastChild.getElementsByTagName('input');
    var oIn=rows[origRows[i].index].getElementsByTagName('input');
    for (var ins=0; ins< sIn.length; ins++)
    {
      sIn[ins].onclick=oIn[ins].onclick;
      sIn[ins].className=oIn[ins].className;
      sIn[ins].onmouseover=oIn[ins].onmouseover;
      sIn[ins].name=oIn[ins].name;
    }
  }
  var sortedTr = sortedBody.getElementsByTagName("tr");
  var cnt=0;
  for (var r=0; r< sortedTr.length; r++)
  {
    var sortedTd = sortedTr[r].getElementsByTagName("td");
    for (var c=0; c< sortedTd.length; c++)
    {
      sortedTd[c].style.background=(cnt%2) ? "#dfdfdf" : "#fbfbfb";
      setTDmouseActions(sortedTd[c],(cnt%2) ? "#dfdfdf" : "#fbfbfb");
    }
    if (sortedTr[r].className != "hide")
      {cnt++;}
    //sortedTr[r].style.background=(r%2) ? "#ccc" : "#fff";
    //var sortedTd = sortedTr[r].getElementsByTagName("td");
    //for (var c=0; c< sortedTd.length; c++)
    //{sortedTd[c].className=cellStyle[flipflop];}
    //flipflop=(flipflop == 1) ? 0 : 1;
  }
  sortedBody.style.height=body.style.height;sortedBody.height=body.height;
  table.replaceChild(sortedBody,body);
//  if (window.addEventListener)
//      {sortedBody.addEventListener("DOMMouseScroll",function(evt){wheel(sortedBody,evt);},false);}
  return false;
}
function setTDmouseActions(obj,color)
{
  obj.onmouseover=function(){markRow(this,"#fbfb66");};
  obj.onmouseout= function(){markRow(this,color);}
}
function Compare(x,y)
{
  var xval=x.value ? x.value.toUpperCase() : x.toUpperCase();
  var yval=y.value ? y.value.toUpperCase() : y.toUpperCase();
  return (xval == yval ? 0 : (xval > yval ? 1 : -1));
}
function CompareCase(x,y)
{
  var xval=x.value;
  var yval=y.value;
  return (xval == yval ? 0 : (xval > yval ? 1 : -1));
}
function CompareDigits(x,y)
{
  x.value=x.value.replace(/[:\/\s]/g,"");
  y.value=y.value.replace(/[:\/\s]/g,"");
  var xval=(x.value != "") ? parseFloat(x.value) : 0 ;
  var yval=(y.value != "") ? parseFloat(y.value) : 0 ;
  return (xval-yval);
} 
function CompareDates(x,y)
{
  var xval=toDate(x.value);
  var yval=toDate(y.value);
  return (xval-yval);
}
function toDate(s) 
{
  var parts = s.split(/[\/\.\s-]/);
  if (parts.length == 3)
  {
    var d = new Date(0);
    if (parts[0].length == 4)
    {
      d.setFullYear(parts[0]);
      d.setMonth(parts[1] - 1);
      d.setDate(parts[2]);
    }
    else
    {
      d.setMonth(parts[0] - 1);
      d.setDate(parts[1]);
      d.setFullYear(parts[2]);
    }
    return d.valueOf();
  }
  else
    {return 0;}
}

