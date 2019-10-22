package jdbc

import java.sql.{Connection, DriverManager}
import guinai.utils.Conf

object ConnJdbc extends Conf{

  private var connection: Connection = _

  private val driver = config.getString("jdbc.driver")
  private val url = config.getString("jdbc.url")
  private val username = config.getString("jdbc.username")
  private val password = config.getString("jdbc.passowrd")
  
  

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
