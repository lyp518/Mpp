package jdbc

import java.sql.{Connection, DriverManager}

object ConnJdbc {

  private var connection: Connection = _

  private val driver = "com.mysql.cj.jdbc.Driver"
  private val url = "jdbc:mysql://hadoop102:3306/company?useUnicode=true&characterEncoding=utf-8&useSSL=false"
  private val username = "root"
  private val password = "123456"

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
