package EsDataProcess

import guinai.utils.Conf

object ESUtils extends Conf{
  val esHttpPort = config.getString("elasticsearch.clusterHttpURI")
  val alias_suffix = config.getString("elasticsearch.alias_suffix")
  val search_suffix = """-*"""
  
  def getAlias(protocalName: String) = {
    protocalName+alias_suffix
  }
  
  def getSearchName(protocalName: String) = {
    protocalName+search_suffix
  }
}