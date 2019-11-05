package EsDataProcess

import com.typesafe.config.{Config, ConfigFactory}
import java.io.File

object ESUtils{
  val config: Config = ConfigFactory.load("MppConfig.conf")
  val esHttpPort = config.getString("elasticsearch.clusterHttpURI")
  val pagesize = config.getInt("elasticsearch.pagesize")
  
  val driver = config.getString("jdbc.driver")
  val url = config.getString("jdbc.url")
  val username = config.getString("jdbc.username")
  val password = config.getString("jdbc.passowrd")
}