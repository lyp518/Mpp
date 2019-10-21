package EsDataProcess

import java.io.PrintWriter
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object EsTest {
  def main(args: Array[String]): Unit = {
    val pagesize = 10
    
    val mustConditions = Array[AbstractCondition]()
  
    var Tag = ""
    
    if(args.length==1){
      Tag = args(0)
    }
    
    Deal_task_ippair_statistics(Tag, pagesize)
  }
  
  def Deal_task_ippair_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_ippair_statistics", Tag , pagesize)
    var i = 0
    
    val time1=System.currentTimeMillis()
    val write = new PrintWriter("task_ippair_statistics" + Tag + time1 + ".csv")
    
    while (res._3>0){
      //println(res._3)
      //println(res._2.toBuffer)
      println(i)
      i = i + 1
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val SrcPort = (jValue\"ipPair")(0).values
        val DstPort = (jValue\"ipPair")(1).values
        val recvBytes = (jValue\"recvBytes").values
        val recvPkts = (jValue\"recvPkts").values
        val sendBytes = (jValue\"sendBytes").values
        val sendPkts = (jValue\"sendPkts").values
        val srcPorts = (jValue\"srcPorts").values
        val dstPorts = (jValue\"dstPorts").values
        val rsByteRate = (jValue\"rsByteRate").values
        val rsPktRate = (jValue\"rsPktRate").values
        val Tag = (jValue\"Tag").values
        val componentID = (jValue\"componentID").values
        
        val result1 = SrcPort + "," + DstPort + "," + recvBytes + "," + recvPkts + "," + sendBytes + "," + sendPkts + ","
        val result2 = srcPorts + "," + dstPorts + "," + rsByteRate + "," + rsPktRate + "," + Tag + "," + componentID + "\n"
        val result = result1 + result2
        
        write.println(result)
      }
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    
    write.close()
    searcher.close
  }
}