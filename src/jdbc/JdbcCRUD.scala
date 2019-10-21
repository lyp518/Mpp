package jdbc

import java.sql.{PreparedStatement, ResultSet, Statement}

object JdbcCRUD {

  val statement: Statement = ConnJdbc.conn().createStatement

  /**
    * 创建数据表
    * @param sql 创建数据表语句
    * @return 返回值判断是否成功，如果创建成功返回0， 否则返回-1
    */
  def createTable(sql: String): Int = {
    try{
      statement.executeUpdate(sql)
    }catch {
      case exception: Exception=>{
        return -1
      }
    }
  }


  /**
    * 删除数据表
    * @param tableName 要删除的表的名称
    * @return 返回值判断是否成功，如果删除成功返回0， 否则返回-1
    */
  def dropTable(tableName: String):Int = {
    try{
      statement.executeUpdate("DROP TABLE "+tableName)
    }catch {
      case exception: Exception=>{
        println(exception.fillInStackTrace())
        return -1
      }
    }
  }

  /**
    * 单条插入数据，直接写sql，没有填充步骤
    * @param sql sql语句，比如"INSERT INTO `scala_jdbc_test`(`name`, `age`, `address`) VALUES ( '李四', 2, '南京')"
    */
  def insert(sql:String): Unit = {
    try{
      val insertNum: Int = statement.executeUpdate(sql)
      println("插入数据，返回值=>" + insertNum)
    }catch {
      case exception: Exception=>{
        println(exception.fillInStackTrace())
      }
    }
  }

  /**
    *
    * @param batchSize 批量插入，每次插入的数据量大小，设置为10，则每10条数据插入一次
    * @param sql 要插入的sql语句，比如"INSERT INTO `scala_jdbc_test`(`name`, `age`, `address`) VALUES ( ?, ?, ?)"
    * @param datas 要插入的数据list，类型为List[List[String]]，内部具体的类型会进行转换
    * @param dataType 设置每个字段的数据类型, 目前只有Int, String, Long, Double
    * @return successNum 返回成功执行的条数
    * @example val sql:String = "INSERT INTO `scala_jdbc_test`(`name`, `age`, `address`) VALUES ( ?, ?, ?)"
    *          val dataType:List[String] = List("String", "Int", "String")
    *          var datas: List[List[String]] = List()
    *          for (i <- 0 to 10){
    *          val data: List[String] = List("name"+i, (20+i).toString, "place"+i)
    *          datas = datas :+ data
    *          }
    *          batchInsert(3,sql, datas, dataType)
    **/
  def batchInsert(batchSize:Int, sql:String, datas: List[List[String]], dataType:List[String]): Int ={
    var rem:Int = 0
    var successNum = 0   // 记录成功执行数据条数
    try{
      val ps: PreparedStatement = ConnJdbc.conn().prepareStatement(sql)
      for(oneData <- datas){
        rem += 1
        for (i <- 1 to dataType.length){
          val thisType:String = dataType(i-1)
          thisType match {
            case "String" => ps.setString(i, oneData(i-1))
            case "Int" => ps.setInt(i, oneData(i-1).toInt)
            case "Long" => ps.setLong(i, oneData(i-1).toLong)
            case "Double" => ps.setDouble(i, oneData(i-1).toDouble)
            case _ => return successNum
          }
        }
        ps.addBatch()
        if(rem==batchSize){
          ps.executeBatch()
          successNum += rem
          rem = 0
        }
      }
      if(rem%batchSize!=0){
        ps.executeBatch()
        successNum += rem
      }
      return successNum
    }catch {
      case exception: Exception=>{
        println(exception.fillInStackTrace())
        return successNum
      }
    }
  }


  /**
    * 查询语句
    *
    * @param sql 传入sql语句，比如"select * from scala_jdbc_test"
    * @param columnNum 传入一共有多少列，因为要循环从中读出，所以一定要写
    * @return 返回查询结果，所有的列返回的都是String类型，用时可以自己转换
    * @example val sql2:String = "select * from scala_jdbc_test"
    *          select(sql2, 4)
    */
  def select(sql: String, columnNum: Int): List[List[String]] = {
    var result: List[List[String]] = List()
    try{
      val res: ResultSet = statement.executeQuery(sql)

      while (res.next) {
        var oneData: List[String] = List()
        for (i <- 0 until columnNum){
          oneData = oneData :+ res.getString(i+1)
        }
        println(oneData)
        result = result :+ oneData

        //      val id = res.getString("id")
        //      val name = res.getString("name")
        //      val age = res.getString("age")
        //      val address = res.getString("address")
        //
        //      println("查询数据")
        //      println("id=%s,name=%s,age=%s,address=%s".format(id, name, age, address))
      }
    }catch {
      case exception: Exception=>{
        println(exception.fillInStackTrace())
      }
    }
    return result
  }

//   def main(args: Array[String]): Unit = {
//
//     //      create table
//     val sql:String = "CREATE TABLE `scala_jdbc_test` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT,`name` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL, `age` tinyint(1) DEFAULT NULL, `address` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";
//     println(createTable(sql))
//
////     // batch insert
////     val sql:String = "INSERT INTO `scala_jdbc_test`(`name`, `age`, `address`) VALUES ( ?, ?, ?)"
////     val dataType:List[String] = List("String", "Int", "String")
////     var datas: List[List[String]] = List()
////     for (i <- 0 to 10){
////       val data: List[String] = List("name"+i, (20+i).toString, "place"+i)
////       println(s"List${data}")
////       datas = datas :+ data
////     }
////     datas = datas :+ List("anm", "a", "bbb")
////     println(batchInsert(3,sql, datas, dataType))
//
////     // search data
////     val sql2:String = "select * from scala_jdbc_test"
////     select(sql2, 4)
//
//
//
//     println(dropTable("scala_jdbc_test"))
//   }

}
