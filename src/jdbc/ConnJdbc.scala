package jdbc

import java.sql.{Connection, DriverManager}
import EsDataProcess.ESUtils

object ConnJdbc{

  private var connection: Connection = _

  private val driver = ESUtils.driver
  private val url = ESUtils.url
  private val username = ESUtils.username
  private val password = ESUtils.password
  
  

  /**
    * 创建mysql连接
    *
    * @return
    */
  def conn(): Connection = {

    if (connection == null) {
      println(this.driver)
      Class.forName(this.driver)
      connection = DriverManager.getConnection(this.url, this.username, this.password)
    }
    connection
  }


}
