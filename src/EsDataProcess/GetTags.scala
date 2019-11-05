package EsDataProcess

import scala.io.Source

object GetTags {
  
  /**
   * 从txt中按行读取Tag,文本格式一行一个Tag，回车换行
 * @param filePath 传入文件路径
 */
def readtxt(filePath:String) = {
    val source = Source.fromFile(filePath, "UTF-8")
    var lines = source.getLines().toArray
    source.close()
    lines
  }
}