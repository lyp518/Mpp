package EsDataProcess

import guinai.utils.Conf

object ESUtils extends Conf{
  val esHttpPort = config.getString("elasticsearch.clusterHttpURI")
  val alias_suffix = config.getString("elasticsearch.alias_suffix")
  val pagesize = config.getInt("elasticsearch.pagesize")
  val search_suffix = """-*"""
}