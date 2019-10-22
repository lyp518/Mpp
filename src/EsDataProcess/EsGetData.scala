package EsDataProcess

import org.elasticsearch.client.Request
import org.elasticsearch.client.RestClient
import org.apache.http.HttpHost
import org.apache.http.util.EntityUtils
import org.elasticsearch.script.mustache.SearchTemplateRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.script.ScriptType
import org.elasticsearch.client.RequestOptions
import java.util.HashMap
import org.elasticsearch.client.RestHighLevelClient
import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.action.search.SearchScrollRequest
import org.elasticsearch.action.search.ClearScrollRequest
import org.elasticsearch.search.Scroll
import org.elasticsearch.common.unit.TimeValue
import org.apache.logging.log4j.LogManager
import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.index.query.QueryBuilders

abstract class AbstractCondition {
  def jsonValue: JValue
  def json: String = compact(render(jsonValue))
}

class EsGetData {
  
  private val log = LogManager.getLogger(this.getClass.getName())  
  
  val client = getConnection
  @UnitTested
  def clearScroll(scrollID: String) = {
    val clearScrollRequest = new ClearScrollRequest() 
    clearScrollRequest.addScrollId(scrollID)
    val clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
    val succeeded = clearScrollResponse.isSucceeded();
    //println("the result of release is: " + succeeded)
  }
  @UnitTested
  def scorllSearch(scrollID: String) = {    
    val scroll = new Scroll(TimeValue.timeValueMinutes(5L))    
    val scrollRequest = new SearchScrollRequest(scrollID) 
    scrollRequest.scroll(scroll)
    var searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT)
    var scrollId = searchResponse.getScrollId
    var hits = searchResponse.getHits.getHits  
    //println(hits.toBuffer);
    //println(searchResponse.getHits.getTotalHits.value)
    log.debug(s"ES scroll $scrollID search returns searchResponse.getHits.getTotalHits.value data")
    (scrollId, hits.map(_.getSourceAsString),hits.length)
  }
  
  @UnitTested
  def scrollSearch(indexName: String,Tag: String, scrollSize: Int) = {
    val scroll = new Scroll(TimeValue.timeValueMinutes(5L))
    val searchRequest = new SearchRequest(indexName)
    val searchSourceBuilder = new SearchSourceBuilder()        
    val query = QueryBuilders.boolQuery()
    if(Tag != ""){
      query.must(QueryBuilders.matchQuery("Tag", Tag))
    }
    searchSourceBuilder.query(query)    
    searchSourceBuilder.size(scrollSize) 
    searchRequest.scroll(scroll)
    searchRequest.source(searchSourceBuilder)    
    var searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
    var scrollId = searchResponse.getScrollId();
    val hitss = searchResponse.getHits()
    var hits = hitss.getHits    
    //println(hits.toBuffer);
    //println(hitss.getTotalHits.value)
    (scrollId, hits.map(_.getSourceAsString),hits.length)
  }
  
  @UnitTested
 def getConnection = {
    val map = ESUtils.esHttpPort.split(",").map(x => {
      val ipport = x.split(":")
      new HttpHost(ipport(0), Integer.parseInt(ipport(1)), "http")
    })
    new RestHighLevelClient(RestClient.builder(map:_*))                
  }
  
  def close = client.close
}