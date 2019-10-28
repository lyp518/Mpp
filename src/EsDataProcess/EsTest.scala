package EsDataProcess

import java.io.PrintWriter
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import java.text.SimpleDateFormat
import java.util.Date
import guinai.utils.Conf

object EsTest {
  def main(args: Array[String]): Unit = {
    val pagesize = ESUtils.pagesize  
    var Tag = "" 
    if(args.length==1){
      Tag = args(0)
    }
    
    Deal_task_ippair_statistics(Tag, pagesize)//有数组
    Deal_task_port_statistics(Tag, pagesize)
    Deal_task_protocol_component_statistics(Tag, pagesize)
    Deal_task_connected_component(Tag, pagesize)
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
    //val write = new PrintWriter("task_ippair_statistics" + time1 + ".csv")

    val dataType:List[String] = List("Long","Long","Long","Long","Long","Long","Long","Long","Float","Float","String","Long")
    
    while (res._3>0){
      println(i)
      i = i + 1
      var datas:List[List[String]] = List()
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val SrcPort = (jValue\"ipPair")(0).values.toString()
        val DstPort = (jValue\"ipPair")(1).values.toString()
        val recvBytes = (jValue\"recvBytes").values.toString()
        val recvPkts = (jValue\"recvPkts").values.toString()
        val sendBytes = (jValue\"sendBytes").values.toString()
        val sendPkts = (jValue\"sendPkts").values.toString()
        val srcPortsCount = (jValue\"srcPortsCount").values.toString()
        val dstPortsCount = (jValue\"dstPortsCount").values.toString()
        val rsByteRate = ((jValue\"rsByteRate").values)
        var strrsByteRate:String = ""
        if(rsByteRate!=null){
          strrsByteRate = rsByteRate.toString()
        }
        val rsPktRate = (jValue\"rsPktRate").values
        var strrsPktRate:String = ""
        if(rsPktRate!=null){
          strrsPktRate = rsPktRate.toString()
        }
        val Tag = (jValue\"Tag").values.toString()
        val componentID = (jValue\"componentID").values.toString()
        
        val data:List[String] = List(SrcPort,DstPort,recvBytes,recvPkts,sendBytes,sendPkts,srcPortsCount,dstPortsCount,strrsByteRate,strrsPktRate,Tag,componentID)
        datas = datas :+ data
//        val result1 = SrcPort + "," + DstPort + "," + recvBytes + "," + recvPkts + "," + sendBytes + "," + sendPkts + ","
//        val result2 = /*srcPorts + "," + dstPorts + "," +*/ rsByteRate + "," + rsPktRate + "," + Tag + "," + componentID
//        val result = result1 + result2
        
//        write.println(result)
      }
      val sql ="""INSERT INTO `task_ippair_statistics`
        (`SrcPort`,`DstPort`,`recvBytes`,`recvPkts`,`sendBytes`,`sendPkts`,`srcPortsCount`,`dstPortsCount`,
        `rsByteRate`,`rsPktRate`,`Tags`,`componentID`)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"""
        
      jdbc.JdbcCRUD.batchInsert(res._3,sql,datas,dataType)
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1)    
    println("task_ippair_statistics finished")
//    write.close()
    searcher.close
  }
  
  def Deal_task_port_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_port_statistics", Tag , pagesize)
    var i = 0
    val time1= NowDate()
 //   val write = new PrintWriter("task_port_statistics" + time1 + ".csv")
    
    val dataType:List[String] = List("String","Long","String","String","Long","Long","Long","Long","Long","Long","Long","Long","Long","Long")
    
    while (res._3>0){
      println(i)
      i = i + 1
      var datas:List[List[String]] = List()
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val Tag = (jValue\"Tag").values.toString()
        val componentID = (jValue\"componentID").values.toString()
        val protocol = (jValue\"protocol").values.toString()
        val topProtocol = (jValue\"topProtocol").values.toString()
        val port = (jValue\"port").values.toString()
        val bytes = (jValue\"bytes").values.toString()
        val packets = (jValue\"packets").values.toString()
        val sendBytes = (jValue\"sendBytes").values.toString()
        val recvBytes = (jValue\"recvBytes").values.toString()
        val sendPkts = (jValue\"sendPkts").values.toString()
        val recvPkts = (jValue\"recvPkts").values.toString()
        val usedIPCount = (jValue\"usedIPCount").values.toString()
        val accessedIPCount = (jValue\"accessedIPCount").values.toString()
        val accessedPortCount = (jValue\"accessedPortCount").values.toString()
       
        val data:List[String] = List(Tag,componentID,protocol,topProtocol,port,bytes,packets,sendBytes,recvBytes,sendPkts,recvPkts,usedIPCount,accessedIPCount,accessedPortCount)
        datas = datas :+ data
        
//        val result1 = Tag + "," + componentID + "," + protocol + "," + topProtocol + "," + port + "," + bytes + "," + packets + ","
//        val result2 = sendBytes + "," + recvBytes + "," + sendPkts + "," + recvPkts + "," + usedIPCount + "," + accessedIPCount + "," + accessedPortCount
//        val result = result1 + result2   
//        write.println(result)
      } 
      val sql ="""INSERT INTO `task_port_statistics`
        (`Tags`,`componentID`,`protocol`,`topProtocol`,`port`,`bytes`,`packets`,`sendBytes`,`recvBytes`,
        `sendPkts`,`recvPkts`,`usedIPCount`,`accessedIPCount`,`accessedPortCount`)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        
      jdbc.JdbcCRUD.batchInsert(res._3,sql,datas,dataType)

      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1)     
    println("task_port_statistics finished")
//    write.close()
    searcher.close
  }
  
//  def Deal_task_ssl_cert_statistics(Tag: String,pagesize: Int){
//    val searcher = new EsGetData
//    var res = searcher.scrollSearch("task_ssl_cert_statistics", Tag , pagesize)
//    var i = 0
//    val time1= NowDate()
//    val write = new PrintWriter("task_ssl_cert_statistics" + time1 + ".csv")
//    
//    while (res._3>0){
//      println(i)
//      i = i + 1
//      for(j<- 0 until res._2.length){
//        val jValue: JValue = parse(res._2(j))
//        
//        val count = (jValue\"count").values
//        val FileLocalUri = (jValue\"FileLocalUri").values
//        val Tag = (jValue\"Tag").values
//        val Ips = (jValue\"Ips").values
//        val hosts = (jValue\"port").values
//        
//        val result = count + "," + FileLocalUri + "," + Tag + "," + Ips + "," + hosts      
//        write.println(result)
//      }   
//      res = searcher.scorllSearch(res._1)
//    }
//    searcher.clearScroll(res._1)     
//    println("task_ssl_cert_statistics finished")
//    write.close()
//    searcher.close
//  }
//  
  def Deal_task_protocol_component_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_protocol_component_statistics", Tag , pagesize)
    var i = 0
    val time1= NowDate()
    //val write = new PrintWriter("task_protocol_component_statistics" + time1 + ".csv")
    val dataType:List[String] = List("String","Long","String","String","Long","Long","Long","Long","Long","Long","Long","Long")
    while (res._3>0){
      println(i)
      i = i + 1
      var datas:List[List[String]] = List()
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j)) 
        
        val Tag = (jValue\"Tag").values.toString()
        val componentID = (jValue\"componentID").values.toString()
        val protocol = (jValue\"protocol").values.toString()
        val topProtocol = (jValue\"topProtocol").values.toString()
        val sendBytes = (jValue\"sendBytes").values.toString()
        val recvBytes = (jValue\"recvBytes").values.toString()
        val bytes = (jValue\"bytes").values.toString()
        val sendPkts = (jValue\"sendPkts").values.toString()
        val recvPkts = (jValue\"recvPkts").values.toString()
        val pkts = (jValue\"pkts").values.toString()
        val ipCount = (jValue\"ipCount").values.toString()
        val portCount = (jValue\"portCount").values.toString()
        
        val data:List[String] = List(Tag,componentID,protocol,topProtocol,sendBytes,recvBytes,bytes,sendPkts,recvPkts,pkts,ipCount,portCount)
        datas = datas :+ data
//        val result1 = Tag + "," + componentID + "," + protocol + "," + topProtocol + "," + sendBytes + "," + recvBytes + ","
//        val result2 = bytes + "," + sendPkts +  "," + recvPkts + "," + pkts + "," + ipCount + "," + portCount 
//        val result = result1 + result2   
//        write.println(result)
      }
      val sql ="""INSERT INTO `task_protocol_component_statistics`
        (`Tags`,`componentID`,`protocol`,`topProtocol`,`sendBytes`,`recvBytes`,`bytes`,
        `sendPkts`,`recvPkts`,`pkts`,`ipCount`,`portCount`)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"""
        
      jdbc.JdbcCRUD.batchInsert(res._3,sql,datas,dataType)
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1)    
    println("task_protocol_component_statistics finished")
//    write.close()
    searcher.close
  }
  
  def Deal_task_connected_component(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_connected_component", Tag , pagesize)
    var i = 0
    val time1= NowDate()
//    val write = new PrintWriter("task_connected_component" + time1 + ".csv")
    val dataType:List[String] = List("String","Long","Long","Long","Long","Long","Long","Long","Long")
    while (res._3>0){
      println(i)
      i = i + 1
      var datas:List[List[String]] = List()
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val Tag = (jValue\"Tag").values.toString()
        val componentID = (jValue\"componentID").values.toString()
        val sendBytes = (jValue\"sendBytes").values.toString()
        val recvBytes = (jValue\"recvBytes").values.toString()
        val sendPkts = (jValue\"sendPkts").values.toString()
        val recvPkts = (jValue\"recvPkts").values.toString()
        val ipCount = (jValue\"ipCount").values.toString()
        val protocolCount = (jValue\"protocolCount").values.toString()
        val topProtocolCount = (jValue\"topProtocolCount").values.toString()
        
        val data:List[String] = List(Tag,componentID,sendBytes,recvBytes,sendPkts,recvPkts,ipCount,protocolCount,topProtocolCount)
        datas = datas :+ data
//        val result1 = Tag + "," + componentID + "," + sendBytes + "," + recvBytes + ","
//        val result2 = sendPkts +  "," + recvPkts + "," + ipCount + "," + protocolCount + "," + topProtocolCount 
//        val result = result1 + result2
//        
//        write.println(result)
      }
      
      val sql ="""INSERT INTO `task_connected_component`
        (`Tags`,`componentID`,`sendBytes`,`recvBytes`,
        `sendPkts`,`recvPkts`,`ipCount`,`protocolCount`,`topProtocolCount`)
        VALUES (?,?,?,?,?,?,?,?,?)"""
        
      jdbc.JdbcCRUD.batchInsert(res._3,sql,datas,dataType)
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    println("task_connected_component finished")
//    write.close()
    searcher.close
  }
  
//  def Deal_task_isakmp_ipvendorpair_statistics(Tag: String,pagesize: Int){
//    val searcher = new EsGetData
//    var res = searcher.scrollSearch("task_isakmp_ipvendorpair_statistics", Tag , pagesize)
//    var i = 0
//    val time1= NowDate()
//    val write = new PrintWriter("task_isakmp_ipvendorpair_statistics" + time1 + ".csv")
//    
//    while (res._3>0){
//      println(i)
//      i = i + 1
//      for(j<- 0 until res._2.length){
//        val jValue: JValue = parse(res._2(j))
//        
//        val SrcIP = (jValue\"SrcIP").values
//        val IPType = (jValue\"IPType").values
//        val VendorID = (jValue\"VendorID").values
//        val Tag = (jValue\"Tag").values
//        
//        val result = SrcIP + "," + IPType + "," + VendorID + "," + Tag
//        write.println(result)
//      }
//      res = searcher.scorllSearch(res._1)
//    }
//    searcher.clearScroll(res._1) 
//    println("task_isakmp_ipvendorpair_statistics finished")
//    write.close()
//    searcher.close
//  }
  
//  def Deal_task_isakmp_vendor_statistics(Tag: String,pagesize: Int){
//    val searcher = new EsGetData
//    var res = searcher.scrollSearch("task_isakmp_vendor_statistics", Tag , pagesize)
//    var i = 0
//    val time1= NowDate()
//    val write = new PrintWriter("task_isakmp_vendor_statistics" + time1 + ".csv")
//    
//    while (res._3>0){
//      println(i)
//      i = i + 1
//      for(j<- 0 until res._2.length){
//        val jValue: JValue = parse(res._2(j))
//        
//        val SrcIP = (jValue\"SrcIP").values
//        val DstIP = (jValue\"DstIP").values
//        val VendorID = (jValue\"VendorID").values
//        val VendorName = (jValue\"VendorName").values
//        val Tag = (jValue\"Tag").values
//        val sessionID = (jValue\"sessionID").values
//        
//        val result = SrcIP + "," + DstIP + "," + VendorID + "," + VendorName + "," + Tag + "," + sessionID
//        write.println(result)
//      }
//      res = searcher.scorllSearch(res._1)
//    }
//    searcher.clearScroll(res._1) 
//    println("task_isakmp_vendor_statistics finished") 
//    write.close()
//    searcher.close
//  }

  def Deal_task_portpair_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_portpair_statistics", Tag , pagesize)
    var i = 0
    val time1= NowDate()
 //   val write = new PrintWriter("task_portpair_statistics" + time1 + ".csv")
    val dataType:List[String] = List("Long","Long","Long","Long","Long","Long","Long","Long","Float","Float","String","Long")
    while (res._3>0){
      println(i)
      i = i + 1
      var datas:List[List[String]] = List()
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val SrcPort = (jValue\"portPair")(0).values.toString()
        val DstPort = (jValue\"portPair")(1).values.toString()
        val srcIpsCount = (jValue\"srcIpsCount").values.toString()
        val dstIpsCount = (jValue\"dstIpsCount").values.toString()
        val recvBytes = (jValue\"recvBytes").values.toString()
        val recvPkts = (jValue\"recvPkts").values.toString()
        val sendBytes = (jValue\"sendBytes").values.toString()
        val sendPkts = (jValue\"sendPkts").values.toString()
        val rsByteRate = ((jValue\"rsByteRate").values)
        var strrsByteRate:String = ""
        if(rsByteRate!=null){
          strrsByteRate = rsByteRate.toString()
        }
        val rsPktRate = (jValue\"rsPktRate").values
        var strrsPktRate:String = ""
        if(rsPktRate!=null){
          strrsPktRate = rsPktRate.toString()
        }
        val Tag = (jValue\"Tag").values.toString()
        val componentID = (jValue\"componentID").values.toString()
        
        val data:List[String] = List(SrcPort,DstPort,srcIpsCount,dstIpsCount,recvBytes,recvPkts,sendBytes,sendPkts,strrsByteRate,strrsPktRate,Tag,componentID)
        datas = datas :+ data
        
//        val result1 = SrcPort + "," + DstPort + "," + /*srcIps + "," + dstIps + ","  +*/ recvBytes + "," + recvPkts + ","
//        val result2 = sendBytes + "," + sendPkts + "," + rsByteRate + "," + rsPktRate + "," + Tag + "," + componentID
//        val result = result1 + result2
//        write.println(result)
      }  
      
      val sql ="""INSERT INTO `task_portpair_statistics`
        (`SrcPort`,`DstPort`,`srcIpsCount`,`dstIpsCount`,`recvBytes`,`recvPkts`,
        `sendBytes`,`sendPkts`,`rsByteRate`,`rsPktRate`,`Tags`,`componentID`)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"""
        
      jdbc.JdbcCRUD.batchInsert(res._3,sql,datas,dataType)
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    println("task_portpair_statistics finished")  
//    write.close()
    searcher.close
  }
  
  def Deal_task_ip_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_ip_statistics", Tag , pagesize)
    var i = 0
    val time1= NowDate()
//    val write = new PrintWriter("task_ip_statistics" + time1 + ".csv")
    val dataType:List[String] = List("String","String","Long","String","String","String","String","String","String","String","Long","Long","Long","Long","Long","Long","Long","Long","Long")
    while (res._3>0){
      println(i)
      i = i + 1
      var datas:List[List[String]] = List()
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val Tag = (jValue\"Tag").values.toString()
        val ip = (jValue\"ip").values.toString()
        val componentID = (jValue\"componentID").values.toString()
        val ipType = (jValue\"ipType").values.toString()
        val country = (jValue\"country").values.toString()
        val city = (jValue\"city").values.toString()
        val connectionType = (jValue\"connectionType").values.toString()
        val domain = (jValue\"domain").values.toString()
        val isp = (jValue\"isp").values.toString()
        val netSeg = (jValue\"netSeg").values.toString()
        val sendBytes = (jValue\"sendBytes").values.toString()
        val recvBytes = (jValue\"recvBytes").values.toString()
        val sendPkts = (jValue\"sendPkts").values.toString()
        val recvPkts = (jValue\"recvPkts").values.toString()
        val relatedIPCount = (jValue\"relatedIPCount").values.toString()
        val usedPortCount = (jValue\"usedPortCount").values.toString()
        val accessedPortCount = (jValue\"accessedPortCount").values.toString()
        val protocolCount = (jValue\"protocolCount").values.toString()
        val topProtocolCount = (jValue\"topProtocolCount").values.toString()
        
        val data:List[String] = List(Tag,ip,componentID,ipType,country,city,connectionType,domain,isp,netSeg,sendBytes,recvBytes,sendPkts,recvPkts,relatedIPCount,usedPortCount,accessedPortCount,protocolCount,topProtocolCount)
        datas = datas :+ data
//        val result1 = Tag + "," + ip + "," + componentID + "," + ipType + ","  + country + "," + city + "," +connectionType + ","
//        val result2 = domain + "," + isp + "," +netSeg + "," + sendBytes + "," + recvBytes + "," + sendPkts + "," + recvPkts + ","
//        val result3 = relatedIPCount + "," + usedPortCount + "," + accessedPortCount + "," + protocolCount + "," + topProtocolCount
//        val result = result1 + result2 + result3
//        write.println(result)
      }
      val sql ="""INSERT INTO `task_ip_statistics`
        (`Tags`,`ip`,`componentID`,`ipType`,`country`,`city`,`connectionType`,`domain`,`isp`,`netSeg`,
        `sendBytes`,`recvBytes`,`sendPkts`,`recvPkts`,`relatedIPCount`,`usedPortCount`,`accessedPortCount`,`protocolCount`,`topProtocolCount`)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        
      jdbc.JdbcCRUD.batchInsert(res._3,sql,datas,dataType)
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1)   
    println("task_ip_statistics finished")
//    write.close()
    searcher.close
  }
  
  def Deal_task_ipportpair_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_ipportpair_statistics", Tag , pagesize)
    var i = 0
    val time1= NowDate()
 //   val write = new PrintWriter("task_ipportpair_statistics" + time1 + ".csv")
    val dataType:List[String] = List("String","Long","String","Long","Long","Long","Long","Long","Float","Float","String","Long")
    while (res._3>0){
      println(i)
      i = i + 1
      var datas:List[List[String]] = List()
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val SrcIPPort = (jValue\"ipPortPair")(0).values
        val DstIPPort = (jValue\"ipPortPair")(1).values
        val SrcMsg = SrcIPPort.toString().split("-")
        val DstMsg = DstIPPort.toString().split("-")
        val SrcIP = SrcMsg(0)
        val SrcPort = SrcMsg(1)
        val DstIP = DstMsg(0)
        val DstPort = DstMsg(1)
        
        val recvBytes = (jValue\"recvBytes").values.toString()
        val recvPkts = (jValue\"recvPkts").values.toString()
        val sendBytes = (jValue\"sendBytes").values.toString()
        val sendPkts = (jValue\"sendPkts").values.toString()
        val rsByteRate = ((jValue\"rsByteRate").values)
        var strrsByteRate:String = ""
        if(rsByteRate!=null){
          strrsByteRate = rsByteRate.toString()
        }
        val rsPktRate = (jValue\"rsPktRate").values
        var strrsPktRate:String = ""
        if(rsPktRate!=null){
          strrsPktRate = rsPktRate.toString()
        }
        val Tag = (jValue\"Tag").values.toString()
        val componentID = (jValue\"componentID").values.toString()
        
        val data:List[String] = List(SrcIP,SrcPort,DstIP,DstPort,recvBytes,recvPkts,sendBytes,sendPkts,strrsByteRate,strrsPktRate,Tag,componentID)
        datas = datas :+ data
//        val result1 = SrcIPPort + "," + DstIPPort + "," + recvBytes + "," + recvPkts + ","
//        val result2 = sendBytes + "," + sendPkts + "," + rsByteRate + "," + rsPktRate + "," + Tag + "," + componentID
//        val result = result1 + result2
//        write.println(result)
      }
      val sql ="""INSERT INTO `task_ipportpair_statistics`
        (`SrcIP`,`SrcPort`,`DstIP`,`DstPort`,`recvBytes`,`recvPkts`,`sendBytes`,`sendPkts`,
        `rsByteRate`,`rsPktRate`,`Tag`,`componentID`)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"""
        
      jdbc.JdbcCRUD.batchInsert(res._3,sql,datas,dataType)
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    println("task_ipportpair_statistics finished")
//    write.close()
    searcher.close
  }
  
  def Deal_task_protocol_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_protocol_statistics", Tag , pagesize)
    var i = 0
    val time1= NowDate()
//    val write = new PrintWriter("task_protocol_statistics" + time1 + ".csv")
    val dataType:List[String] = List("String","Long","String","String","Long","Long","Long","Long","Long","Long","Long","Long")
    while (res._3>0){
      println(i)
      i = i + 1
      var datas:List[List[String]] = List()
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val Tag = (jValue\"Tag").values.toString()
        val componentID = (jValue\"componentID").values.toString()
        val protocol = (jValue\"protocol").values.toString()
        val topProtocol = (jValue\"topProtocol").values.toString()
        val sendBytes = (jValue\"sendBytes").values.toString()
        val recvBytes = (jValue\"recvBytes").values.toString()
        val bytes = (jValue\"bytes").values.toString()
        val sendPkts = (jValue\"sendPkts").values.toString()
        val recvPkts = (jValue\"recvPkts").values.toString()
        val pkts = (jValue\"pkts").values.toString()
        val ipCount = (jValue\"ipCount").values.toString()
        val portCount = (jValue\"portCount").values.toString()
        
        val data:List[String] = List(Tag,componentID,protocol,topProtocol,sendBytes,recvBytes,bytes,sendPkts,recvPkts,pkts,ipCount,portCount)
        datas = datas :+ data
//        val result1 = Tag + "," + componentID + "," + protocol + "," + topProtocol + "," + sendBytes + "," + recvBytes + ","
//        val result2 = bytes + "," + sendPkts +  "," + recvPkts + "," + pkts + "," + ipCount + "," + portCount 
//        val result = result1 + result2       
//        write.println(result)
      }
      val sql ="""INSERT INTO `task_protocol_statistics`
        (`Tags`,`componentID`,`protocol`,`topProtocol`,`sendBytes`,`recvBytes`,`bytes`,`sendPkts`,`recvPkts`,
        `pkts`,`ipCount`,`portCount`)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"""
        
      jdbc.JdbcCRUD.batchInsert(res._3,sql,datas,dataType)
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    println("task_protocol_statistics finished")
//    write.close()
    searcher.close
  }
  
  def Deal_task_port_component_statistics(Tag: String,pagesize: Int){
    val searcher = new EsGetData
    var res = searcher.scrollSearch("task_port_component_statistics", Tag , pagesize)
    var i = 0
    val time1= NowDate()
//    val write = new PrintWriter("task_port_component_statistics" + time1 + ".csv")
    val dataType:List[String] = List("String","Long","String","String","Long","Long","Long","Long","Long","Long","Long","Long")
    while (res._3>0){
      println(i)
      i = i + 1
      var datas:List[List[String]] = List()
      for(j<- 0 until res._2.length){
        val jValue: JValue = parse(res._2(j))
        
        val Tag = (jValue\"Tag").values.toString()
        val componentID = (jValue\"componentID").values.toString()
        val protocol = (jValue\"protocol").values.toString()
        val topProtocol = (jValue\"topProtocol").values.toString()
        val port = (jValue\"port").values.toString()
        val bytes = (jValue\"bytes").values.toString()
        val packets = (jValue\"packets").values.toString()
        val sendBytes = (jValue\"sendBytes").values.toString()
        val recvBytes = (jValue\"recvBytes").values.toString()
        val sendPkts = (jValue\"sendPkts").values.toString()
        val recvPkts = (jValue\"recvPkts").values.toString()
        val usedIPCount = (jValue\"usedIPCount").values.toString()
        val accessedIPCount = (jValue\"accessedIPCount").values.toString()
        val accessedPortCount = (jValue\"accessedPortCount").values.toString()
        
        val data:List[String] = List(Tag,componentID,protocol,topProtocol,port,bytes,packets,sendBytes,recvBytes,sendPkts,recvPkts,usedIPCount,accessedIPCount,accessedPortCount)
        datas = datas :+ data
//        val result1 = Tag + "," + componentID + "," + protocol + "," + topProtocol + "," + port + "," + bytes + "," + packets + ","
//        val result2 = sendBytes + "," + recvBytes + "," + sendPkts + "," + recvPkts + "," + usedIPCount + "," + accessedIPCount + "," + accessedPortCount
//        val result = result1 + result2
//        
//        write.println(result)
      }
      val sql ="""INSERT INTO `task_port_component_statistics`
        (`Tags`,`componentID`,`protocol`,`topProtocol`,`port`,`bytes`,`packets`,`sendBytes`,`recvBytes`,`sendPkts`,`recvPkts`,
        `usedIPCount`,`accessedIPCount`,`accessedPortCount`)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        
      jdbc.JdbcCRUD.batchInsert(res._3,sql,datas,dataType)
      
      res = searcher.scorllSearch(res._1)
    }
    searcher.clearScroll(res._1) 
    println("task_port_component_statistics finished")
//    write.close()
    searcher.close
  }
  
  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = dateFormat.format(now)
    return date
  }
  
}