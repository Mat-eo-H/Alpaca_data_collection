//DragDrop table script
//Author: Allan Betschart
var dragObj = null;
var resizeObj=null;
var mouseXY = null;
var saveFunc = {};
var xLimit=0;
var yLimit=0;
function initDrag(x,y)
{
  xLimit=x ? x : xLimit;
  yLimit=y ? y : yLimit;
  doc.onmousemove = mouseMove;
  doc.onmouseup =   mouseUp;
}
function mouseUp()
{
  if((dragObj)||(resizeObj))
    {setSelect(true);}
  if (dragObj)
    {setOpacity(dragObj,1);}
  dragObj=null;
  if (resizeObj)
    {setOpacity(resizeObj.t,1);}
  resizeObj=null;
}
function mouseMove(evt)
{
  evt = evt||window.event;
  var mPos=mCords(evt);
  if (dragObj)
  {
     dragObj.style.position="absolute";
     dragObj.style.left=(mPos.x - mouseXY.x) > xLimit ? mPos.x - mouseXY.x :xLimit;
     dragObj.style.top=(mPos.y - mouseXY.y) > yLimit ? mPos.y - mouseXY.y : yLimit;
     return false;
  }
  if (resizeObj)
  {
    switch(resizeObj.d)
    {
      case 'NE': var top=parseFloat((!/\%/.test(resizeObj.t.style.top)) ? resizeObj.t.style.top : 0);
                var newTop=parseFloat(((mPos.y - mouseXY.y)) > yLimit ? (mPos.y - mouseXY.y) :yLimit);
                top = ((top == 0)||(isNaN(top))) ? newTop : top;
                resizeObj.t.style.height=(resizeObj.t.clientHeight + (top - newTop) > resizeObj.minH) ? 
                                          resizeObj.t.clientHeight + (top - newTop) : resizeObj.minH;
                resizeObj.t.style.width=(mPos.x - mouseXY.w > resizeObj.minW) ? mPos.x - mouseXY.w : resizeObj.minW;
                resizeObj.t.style.top=(resizeObj.t.clientHeight + (top - newTop) > resizeObj.minH) ? newTop : top;
        break;
      case 'NW': var left=parseFloat((!/\%/.test(resizeObj.t.style.left)) ? resizeObj.t.style.left : 0);
                var newLeft=parseFloat(((mPos.x - mouseXY.x)) > xLimit ? (mPos.x - mouseXY.x) :xLimit);
                resizeObj.t.style.left=newLeft;
                left = ((left == 0)||(isNaN(left))) ? newLeft : left;
                resizeObj.t.style.width=(resizeObj.t.clientWidth + (left - newLeft) > resizeObj.minW) ?
                                         resizeObj.t.clientWidth + (left - newLeft) : resizeObj.minW;
                resizeObj.t.style.left=(resizeObj.t.clientWidth + (left - newLeft) > resizeObj.minW) ? newLeft : left;
      case 'N': var top=parseFloat((!/\%/.test(resizeObj.t.style.top)) ? resizeObj.t.style.top : 0);
                var newTop=parseFloat(((mPos.y - mouseXY.y)) > yLimit ? (mPos.y - mouseXY.y) :yLimit);
                top = ((top == 0)||(isNaN(top))) ? newTop : top;
                resizeObj.t.style.height=(resizeObj.t.clientHeight + (top - newTop) > resizeObj.minH) ? 
                                          resizeObj.t.clientHeight + (top - newTop) : resizeObj.minH;
                resizeObj.t.style.top=(resizeObj.t.clientHeight + (top - newTop) > resizeObj.minH) ? newTop : top;
        break;
      case 'E': resizeObj.t.style.width=(mPos.x - mouseXY.w > resizeObj.minW) ? mPos.x - mouseXY.w : resizeObj.minW;
        break;
      case 'SE': resizeObj.t.style.width=(mPos.x - mouseXY.w > resizeObj.minW) ? mPos.x - mouseXY.w : resizeObj.minW;
      case 'S': resizeObj.t.style.height=(mPos.y - mouseXY.h > resizeObj.minH) ? mPos.y - mouseXY.h : resizeObj.minH;
        break;
      case 'SW': resizeObj.t.style.height=(mPos.y - mouseXY.h > resizeObj.minH) ? mPos.y - mouseXY.h : resizeObj.minH;
      case 'W': var left=parseFloat((!/\%/.test(resizeObj.t.style.left)) ? resizeObj.t.style.left : 0);
                var newLeft=parseFloat(((mPos.x - mouseXY.x)) > xLimit ? (mPos.x - mouseXY.x) :xLimit);
  
                left = ((left == 0)||(isNaN(left))) ? newLeft : left;
                resizeObj.t.style.width=(resizeObj.t.clientWidth + (left - newLeft) > resizeObj.minW) ?
                                         resizeObj.t.clientWidth + (left - newLeft) : resizeObj.minW;
                resizeObj.t.style.left=(resizeObj.t.clientWidth + (left - newLeft) > resizeObj.minW) ? newLeft : left;
        break;
    }
    if (resizeObj.i)
      {resizeObj.i.style.height=resizeObj.t.clientHeight - resizeObj.h;}
  }
}
function mCords(evt)
{
  if ((evt.pageX)||(evt.pageY))
    {return{x:evt.pageX,y:evt.pageY};}
  else
  {
    return{x:evt.clientX + doc.body.scrollLeft - doc.body.clientLeft,
           y:evt.clientY + doc.body.scrollTop - doc.body.clientTop};
  }
}
function getmouseXY(node, evt)
{
  evt = evt||window.event;
  var nPos=getPos(node);
  var mPos=mCords(evt);
  return {x:mPos.x-nPos.x,y:mPos.y-nPos.y,w:mPos.x-nPos.w,h:mPos.y-nPos.h};
}
function getPos(node)
{
  var left=0;
  var top=0;
  var width=node.clientWidth;
  var height=node.clientHeight;
  while(node.offsetParent)
  {
    left+=node.offsetLeft;
    top+=node.offsetTop;
    node=node.offsetParent;
  }
  left+=node.offsetLeft;
  top+=node.offsetTop;
  return {x:left,y:top,w:width,h:height};
}
function makeDraggable(node,target)
{
  if (!node) return;
  node.onmousedown=function(evt)
    {return dragIt(evt,this,target);}
}
function setOpacity(obj,val)
{
    obj.style.opacity=val;
    obj.style.mozopacity=val;
    obj.style.filter="alpha(opacity="+(100*val)+")";
}
function setSelect(bool)
{
  if (bool)
  {
    doc.onmousedown = saveFunc.od;
    doc.onselectstart = saveFunc.oss;
  }
  else
  {
    saveFunc = {od:doc.onmousedown,oss:doc.onselectstart};
    doc.onmousedown = function(){return false;};
    doc.onselectstart = function(){return false;};
  }
}
function dragIt(evt,node,target)
{ 
  if ((evt.button == 2)||(evt.button == 3))
    {return false;}
  if (target)
  {
    dragObj=target;
    mouseXY=getmouseXY(target,evt);
    setOpacity(target,0.8);
  }
  else
  {
    dragObj=node;
    mouseXY=getmouseXY(node,evt);
    setOpacity(node,0.8);
  }
  setSelect(false);
}
function resizeIt(evt,node,target,innerNode,minH,minW)
{
  if (minH == null)
    {minH=0;}
  if (minW == null)
    {minW=0;}
  if ((evt.button == 2)||(evt.button == 3))
    {return false;}
  var hDiff=0;
  if (target && innerNode)
    {hDiff=target.clientHeight - innerNode.clientHeight;}
  var dir=node.style.cursor.replace("-resize","").toUpperCase();
  if (target)
  {
    resizeObj={t:target,i:innerNode,h:hDiff,d:dir,minH:minH,minW:minW};
    mouseXY=getmouseXY(target,evt);
    setOpacity(target,0.8);
  }
  else
  {
    resizeObj={t:node,i:innerNode,h:hDiff,d:dir,minH:minH,minW:minW};
    mouseXY=getmouseXY(node,evt);
    setOpacity(node,0.8);
  }
  setSelect(false);
}
function maxMinWin(node,target,innerNode)
{
  var maximize=(/Max/.test(node.className)) ? true : false;
  if (maximize)
  {
    var targPos=getPos(target);
    var inPos=getPos(innerNode);
    node.setAttribute("currW",targPos.w);
    node.setAttribute("currH",targPos.h);
    node.setAttribute("currH2",inPos.h);
    node.setAttribute("currX",targPos.x);
    node.setAttribute("currY",targPos.y);
    
    var hDiff=0;
    if (target && innerNode)
      {hDiff=target.clientHeight - innerNode.clientHeight;}
    target.style.top=yLimit+"px";
    target.style.left=xLimit+"px";
    target.style.width=doc.body.clientWidth-xLimit;
    target.style.height=doc.body.clientHeight-yLimit;
    innerNode.style.height=doc.body.clientHeight-yLimit-hDiff;
    if(isSafari)
      {node.className=node.className.replace(/Max(H)?/,"Min");}
    else
      {node.className=node.className.replace(/Max/,"Min");}
  }
  else
  {
    target.style.width=node.getAttribute("currW");
    target.style.height=node.getAttribute("currH");
    target.style.top=node.getAttribute("currX");
    target.style.left=node.getAttribute("currY");
    innerNode.style.height=node.getAttribute("currH2");
    if(isSafari)
      {node.className=node.className.replace(/Min(H)?/,"Max");}
    else
      {node.className=node.className.replace(/Min/,"Max");}
  }
}
