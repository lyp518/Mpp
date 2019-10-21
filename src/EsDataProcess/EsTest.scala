package EsDataProcess

import java.io.PrintWriter
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import java.text.SimpleDateFormat
import java.util.Date

object EsTest {
  def main(args: Array[String]): Unit = {
    val pagesize = 10000
    
    val mustConditions = Array[AbstractCondition]()
  
    var Tag = "test"
    
    if(args.length==1){
      Tag = args(0)
    }
    
    Deal_task_ippair_statistics(Tag, pagesize)//有数组
    Deal_task_port_statistics(Tag, pagesize)
    Deal_task_ssl_cert_statistics(Tag, pagesize)//Tag是数组？
    Deal_task_protocol_component_statistics(Tag, pagesize)
    Deal_task_connected_component(Tag, pagesize)
    Deal_task_isakmp_ipvendorpair_statistics(Tag, pagesize)
    Deal_task_isakmp_vendor_statistics(Tag, pagesize)
    Deal_task_portpair_statistics(Tag, pagesize)
    Deal_task_ip_statistics(Tag, pagesize)
    Deal_task_ipportpair_statistics(Tag, pagesize)
    Deal_task_protocol_statistics(Tag, pagesize)
    Deal_task_port_component_statistics(Tag, pagesize)
    
  }
  
  
  def Deal_task_ippair_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_ippair_statistics", Tag , pagesize)
    var i = 0
    
    val time1= NowDate()
    val write = new PrintWriter("task_ippair_statistics-" + time1 + ".csv")
    
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
        val result2 = srcPorts + "," + dstPorts + "," + rsByteRate + "," + rsPktRate + "," + Tag + "," + componentID
        val result = result1 + result2
        
        write.println(result)
      }
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    
    write.close()
    searcher.close
  }
  
  def Deal_task_port_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_port_statistics", Tag , pagesize)
    var i = 0
    
    val time1= NowDate()
    val write = new PrintWriter("task_port_statistics-" + time1 + ".csv")
    
    while (res._3>0){
      //println(res._3)
      //println(res._2.toBuffer)
      println(i)
      i = i + 1
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val Tag = (jValue\"Tag").values
        val componentID = (jValue\"componentID").values
        val protocol = (jValue\"protocol").values
        val topProtocol = (jValue\"topProtocol").values
        val port = (jValue\"port").values
        val bytes = (jValue\"bytes").values
        val packets = (jValue\"packets").values
        val sendBytes = (jValue\"sendBytes").values
        val recvBytes = (jValue\"recvBytes").values
        val sendPkts = (jValue\"sendPkts").values
        val recvPkts = (jValue\"recvPkts").values
        val usedIPCount = (jValue\"usedIPCount").values
        val accessedIPCount = (jValue\"usedIPCount").values
        val accessedPortCount = (jValue\"usedIPCount").values
        //val usedIPCount = (jValue\"usedIPCount").values
        
        val result1 = Tag + "," + componentID + "," + protocol + "," + topProtocol + "," + port + "," + bytes + "," + packets + ","
        val result2 = sendBytes + "," + recvBytes + "," + sendPkts + "," + recvPkts + "," + usedIPCount + "," + accessedIPCount + "," + accessedPortCount
        val result = result1 + result2
        
        write.println(result)
      }
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    
    write.close()
    searcher.close
  }
  
  def Deal_task_ssl_cert_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_ssl_cert_statistics", Tag , pagesize)
    var i = 0
    
    val time1= NowDate()
    val write = new PrintWriter("task_ssl_cert_statistics-" + time1 + ".csv")
    
    while (res._3>0){
      //println(res._3)
      //println(res._2.toBuffer)
      println(i)
      i = i + 1
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val count = (jValue\"count").values
        val FileLocalUri = (jValue\"FileLocalUri").values
        val Tag = (jValue\"Tag").values
        val Ips = (jValue\"Ips").values
        val hosts = (jValue\"port").values
        
        val result = count + "," + FileLocalUri + "," + Tag + "," + Ips + "," + hosts
        
        write.println(result)
      }
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    
    write.close()
    searcher.close
  }
  
  def Deal_task_protocol_component_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_protocol_component_statistics", Tag , pagesize)
    var i = 0
    
    val time1= NowDate()
    val write = new PrintWriter("task_protocol_component_statistics-" + time1 + ".csv")
    
    while (res._3>0){
      //println(res._3)
      //println(res._2.toBuffer)
      println(i)
      i = i + 1
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val Tag = (jValue\"Tag").values
        val componentID = (jValue\"componentID").values
        val protocol = (jValue\"protocol").values
        val topProtocol = (jValue\"topProtocol").values
        val sendBytes = (jValue\"sendBytes").values
        val recvBytes = (jValue\"recvBytes").values
        val bytes = (jValue\"bytes").values
        val sendPkts = (jValue\"sendPkts").values
        val recvPkts = (jValue\"recvPkts").values
        val pkts = (jValue\"pkts").values
        val ipCount = (jValue\"ipCount").values
        val portCount = (jValue\"portCount").values
        
        val result1 = Tag + "," + componentID + "," + protocol + "," + topProtocol + "," + sendBytes + "," + recvBytes + ","
        val result2 = bytes + "," + sendPkts +  "," + recvPkts + "," + pkts + "," + ipCount + "," + portCount 
        val result = result1 + result2
        
        write.println(result)
      }
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    
    write.close()
    searcher.close
  }
  
  def Deal_task_connected_component(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_connected_component", Tag , pagesize)
    var i = 0
    
    val time1= NowDate()
    val write = new PrintWriter("task_connected_component-" + time1 + ".csv")
    
    while (res._3>0){
      //println(res._3)
      //println(res._2.toBuffer)
      println(i)
      i = i + 1
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val Tag = (jValue\"Tag").values
        val componentID = (jValue\"componentID").values
        val sendBytes = (jValue\"sendBytes").values
        val recvBytes = (jValue\"recvBytes").values
        val sendPkts = (jValue\"sendPkts").values
        val recvPkts = (jValue\"recvPkts").values
        val ipCount = (jValue\"ipCount").values
        val protocolCount = (jValue\"protocolCount").values
        val topProtocolCount = (jValue\"topProtocolCount").values
        
        val result1 = Tag + "," + componentID + "," + sendBytes + "," + recvBytes + ","
        val result2 = sendPkts +  "," + recvPkts + "," + ipCount + "," + protocolCount + "," + topProtocolCount 
        val result = result1 + result2
        
        write.println(result)
      }
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    
    write.close()
    searcher.close
  }
  
  def Deal_task_isakmp_ipvendorpair_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_isakmp_ipvendorpair_statistics", Tag , pagesize)
    var i = 0
    
    val time1= NowDate()
    val write = new PrintWriter("task_isakmp_ipvendorpair_statistics-" + time1 + ".csv")
    
    while (res._3>0){
      //println(res._3)
      //println(res._2.toBuffer)
      println(i)
      i = i + 1
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val SrcIP = (jValue\"SrcIP").values
        val IPType = (jValue\"IPType").values
        val VendorID = (jValue\"VendorID").values
        val Tag = (jValue\"Tag").values
        
        val result = SrcIP + "," + IPType + "," + VendorID + "," + Tag
        
        write.println(result)
      }
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    
    write.close()
    searcher.close
  }
  
  def Deal_task_isakmp_vendor_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_isakmp_vendor_statistics", Tag , pagesize)
    var i = 0
    
    val time1= NowDate()
    val write = new PrintWriter("task_isakmp_vendor_statistics-" + time1 + ".csv")
    
    while (res._3>0){
      //println(res._3)
      //println(res._2.toBuffer)
      println(i)
      i = i + 1
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val SrcIP = (jValue\"SrcIP").values
        val DstIP = (jValue\"DstIP").values
        val VendorID = (jValue\"VendorID").values
        val VendorName = (jValue\"VendorName").values
        val Tag = (jValue\"Tag").values
        val sessionID = (jValue\"sessionID").values
        
        val result = SrcIP + "," + DstIP + "," + VendorID + "," + VendorName + "," + Tag + "," + sessionID
        
        write.println(result)
      }
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    
    write.close()
    searcher.close
  }
  
  //有数组
  def Deal_task_portpair_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_portpair_statistics", Tag , pagesize)
    var i = 0
    
    val time1= NowDate()
    val write = new PrintWriter("task_portpair_statistics-" + time1 + ".csv")
    
    while (res._3>0){
      //println(res._3)
      //println(res._2.toBuffer)
      println(i)
      i = i + 1
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val SrcIP = (jValue\"portPair")(0).values
        val DstIP = (jValue\"portPair")(1).values
        val srcIps = (jValue\"srcIps").values
        val dstIps = (jValue\"dstIps").values
        val recvBytes = (jValue\"recvBytes").values
        val recvPkts = (jValue\"recvPkts").values
        val sendBytes = (jValue\"sendBytes").values
        val sendPkts = (jValue\"sendPkts").values
        val rsByteRate = (jValue\"rsByteRate").values
        val rsPktRate = (jValue\"rsPktRate").values
        val Tag = (jValue\"Tag").values
        val componentID = (jValue\"componentID").values
        
        val result1 = SrcIP + "," + DstIP + "," + srcIps + "," + dstIps + ","  + recvBytes + "," + recvPkts + ","
        val result2 = sendBytes + "," + sendPkts + "," + rsByteRate + "," + rsPktRate + "," + Tag + "," + componentID
        val result = result1 + result2
        
        write.println(result)
      }
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    
    write.close()
    searcher.close
  }
  
  def Deal_task_ip_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_ip_statistics", Tag , pagesize)
    var i = 0
    
    val time1= NowDate()
    val write = new PrintWriter("task_ip_statistics-" + time1 + ".csv")
    
    while (res._3>0){
      //println(res._3)
      //println(res._2.toBuffer)
      println(i)
      i = i + 1
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val Tag = (jValue\"Tag")(0).values
        val ip = (jValue\"ip")(1).values
        val componentID = (jValue\"componentID").values
        val ipType = (jValue\"ipType").values
        val country = (jValue\"country").values
        val city = (jValue\"city").values
        val connectionType = (jValue\"connectionType").values
        val domain = (jValue\"domain").values
        val isp = (jValue\"isp").values
        val netSeg = (jValue\"netSeg").values
        val sendBytes = (jValue\"sendBytes").values
        val recvBytes = (jValue\"recvBytes").values
        val sendPkts = (jValue\"sendPkts").values
        val recvPkts = (jValue\"recvPkts").values
        val relatedIPCount = (jValue\"relatedIPCount").values
        val usedPortCount = (jValue\"usedPortCount").values
        val accessedPortCount = (jValue\"accessedPortCount").values
        val protocolCount = (jValue\"protocolCount").values
        val topProtocolCount = (jValue\"topProtocolCount").values
        
        val result1 = Tag + "," + ip + "," + componentID + "," + ipType + ","  + country + "," + city + "," +connectionType + ","
        val result2 = domain + "," + isp + "," +netSeg + "," + sendBytes + "," + recvBytes + "," + sendPkts + "," + recvPkts + ","
        val result3 = relatedIPCount + "," + usedPortCount + "," + accessedPortCount + "," + protocolCount + "," + topProtocolCount
        val result = result1 + result2 + result3
        
        write.println(result)
      }
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    
    write.close()
    searcher.close
  }
  
  def Deal_task_ipportpair_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_ipportpair_statistics", Tag , pagesize)
    var i = 0
    
    val time1= NowDate()
    val write = new PrintWriter("task_ipportpair_statistics-" + time1 + ".csv")
    
    while (res._3>0){
      //println(res._3)
      //println(res._2.toBuffer)
      println(i)
      i = i + 1
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val SrcIPPort = (jValue\"ipPortPair")(0).values
        val DstIPPort = (jValue\"ipPortPair")(1).values
        val recvBytes = (jValue\"recvBytes").values
        val recvPkts = (jValue\"recvPkts").values
        val sendBytes = (jValue\"sendBytes").values
        val sendPkts = (jValue\"sendPkts").values
        val rsByteRate = (jValue\"rsByteRate").values
        val rsPktRate = (jValue\"rsPktRate").values
        val Tag = (jValue\"Tag").values
        val componentID = (jValue\"componentID").values
        
        val result1 = SrcIPPort + "," + DstIPPort + "," + recvBytes + "," + recvPkts + ","
        val result2 = sendBytes + "," + sendPkts + "," + rsByteRate + "," + rsPktRate + "," + Tag + "," + componentID
        val result = result1 + result2
        
        write.println(result)
      }
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    
    write.close()
    searcher.close
  }
  
  def Deal_task_protocol_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_protocol_statistics", Tag , pagesize)
    var i = 0
    
    val time1= NowDate()
    val write = new PrintWriter("task_protocol_statistics-" + time1 + ".csv")
    
    while (res._3>0){
      //println(res._3)
      //println(res._2.toBuffer)
      println(i)
      i = i + 1
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val Tag = (jValue\"Tag").values
        val componentID = (jValue\"componentID").values
        val protocol = (jValue\"protocol").values
        val topProtocol = (jValue\"topProtocol").values
        val sendBytes = (jValue\"sendBytes").values
        val recvBytes = (jValue\"recvBytes").values
        val bytes = (jValue\"bytes").values
        val sendPkts = (jValue\"sendPkts").values
        val recvPkts = (jValue\"recvPkts").values
        val pkts = (jValue\"pkts").values
        val ipCount = (jValue\"ipCount").values
        val portCount = (jValue\"portCount").values
        
        val result1 = Tag + "," + componentID + "," + protocol + "," + topProtocol + "," + sendBytes + "," + recvBytes + ","
        val result2 = bytes + "," + sendPkts +  "," + recvPkts + "," + pkts + "," + ipCount + "," + portCount 
        val result = result1 + result2
        
        write.println(result)
      }
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    
    write.close()
    searcher.close
  }
  
  def Deal_task_port_component_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_port_statistics", Tag , pagesize)
    var i = 0
    
    val time1= NowDate()
    val write = new PrintWriter("task_port_statistics-" + time1 + ".csv")
    
    while (res._3>0){
      //println(res._3)
      //println(res._2.toBuffer)
      println(i)
      i = i + 1
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val Tag = (jValue\"Tag").values
        val componentID = (jValue\"componentID").values
        val protocol = (jValue\"protocol").values
        val topProtocol = (jValue\"topProtocol").values
        val port = (jValue\"port").values
        val bytes = (jValue\"bytes").values
        val packets = (jValue\"packets").values
        val sendBytes = (jValue\"sendBytes").values
        val recvBytes = (jValue\"recvBytes").values
        val sendPkts = (jValue\"sendPkts").values
        val recvPkts = (jValue\"recvPkts").values
        val usedIPCount = (jValue\"usedIPCount").values
        val accessedIPCount = (jValue\"usedIPCount").values
        val accessedPortCount = (jValue\"usedIPCount").values
        
        val result1 = Tag + "," + componentID + "," + protocol + "," + topProtocol + "," + port + "," + bytes + "," + packets + ","
        val result2 = sendBytes + "," + recvBytes + "," + sendPkts + "," + recvPkts + "," + usedIPCount + "," + accessedIPCount + "," + accessedPortCount
        val result = result1 + result2
        
        write.println(result)
      }
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    
    write.close()
    searcher.close
  }
  
  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = dateFormat.format(now)
    return date
  }
  
}