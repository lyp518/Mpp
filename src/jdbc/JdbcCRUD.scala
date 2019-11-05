package jdbc

import java.sql.{PreparedStatement, ResultSet, Statement}

object JdbcCRUD {

  val statement: Statement = null//ConnJdbc.conn().createStatement

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
   * @param tableName 要删除数据的表
   * @param conditions 删除数据的条件，满足条件则删除
   * 删除数据
   */
  def delete(tableName: String, conditions: String=null):Unit = {
    var sql: String = s"DELETE FROM ${tableName} "
    if(conditions != null){
      sql += s"WHERE ${conditions}"
    }else{
      throw new Exception("删除条件为空，请查询删除函数中的条件参数是否传递错误")
    }
//    if(conditions.length>0){
//      sql += s"WHERE ${conditions(0)}"
//      for(i <- 1 until conditions.length)
//        sql += s" AND ${conditions(i)}"
//    }
    println(sql)
    try{
      statement.executeUpdate(sql)
    }catch{
      case exception: Exception=>{
        println(exception.fillInStackTrace())
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
      ConnJdbc.conn().setAutoCommit(false);
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
            case "Float" => ps.setFloat(i, oneData(i-1).toFloat)
            case _ => return successNum
          }
        }
        ps.addBatch()
        if(rem==batchSize){
          ps.executeBatch()
          ConnJdbc.conn().commit()
          ps.clearBatch()
          successNum += rem
          rem = 0
        }
      }
      if(rem%batchSize!=0){
        ps.executeBatch()
        ConnJdbc.conn().commit()
        ps.clearBatch()
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
    * @param tableName 要查询的表名"
    * @param columns 要查询的列组成的list，比如List(age, name)，如果全查，可以不填，默认为*
    * @param conditions 要查询的条件，默认为null也就是全查
    * @return 返回查询结果，所有的列返回的都是String类型，用时可以自己转换
    * @example select("scala_jdbc_test", List("age", "name"), "id>5")
    */
  def select(tableName: String, columns: List[String]=List("*"), conditions: String=null): List[List[String]] = {
    
    var result: List[List[String]] = List()
    try{
      var sql = s"SELECT ${columns.mkString(",")} FROM ${tableName}"
      if(conditions!=null){
        sql += s" WHERE ${conditions}"
      }
      val res: ResultSet = statement.executeQuery(sql)
      val columnNum: Int = res.getMetaData.getColumnCount
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

//     //      create table
//     val sqlo:String = "CREATE TABLE `scala_jdbc_test` (`id` int(10) unsigned NOT NULL AUTO_INCREMENT,`name` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL, `age` tinyint(1) DEFAULT NULL, `address` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";
//     println(createTable(sqlo))
//
//     // batch insert
//     val sql:String = "INSERT INTO `scala_jdbc_test`(`name`, `age`, `address`) VALUES ( ?, ?, ?)"
//     val dataType:List[String] = List("String", "Int", "String")
//     var datas: List[List[String]] = List()
//     for (i <- 0 to 10){
//       val data: List[String] = List("name"+i, (20+i).toString, "place"+i)
//       println(s"List${data}")
//       datas = datas :+ data
//     }
//     println(batchInsert(3,sql, datas, dataType))

//     // search data
//     val sql2:String = "select * from scala_jdbc_test"
//     select(sql2, 4)
     
     
//     val sql2:String = "select * from scala_jdbc_test"
//     select("scala_jdbc_test", List("age", "name"), "id>5")
     
     
//     println("----------------------------------------------------------")
//     
//     delete("scala_jdbc_test", "age=21 or age>28")
//     
//     println("----------------------------------------------------------")
//     
//     select(sql2)



//     println(dropTable("scala_jdbc_test"))
//   }

}
