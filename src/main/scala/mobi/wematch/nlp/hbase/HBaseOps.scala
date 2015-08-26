package mobi.wematch.nlp.hbase

import mobi.wematch.nlp.http.HttpOps._
import mobi.wematch.nlp.mysql.MySqlOps._
import mobi.wematch.nlp.strops.StrOps._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.PageFilter
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}

//import scala.Predef.Map
//import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

/**
 * Created by cookeem on 15/7/25.
 */
object HBaseOps {
  //Scan的输出格式转换
  def convertScanToString(scan:  Scan)  =  {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  //初始化HBase连接
  def initHbase(): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.master", "hdfs://localhost:60000")
    conf.addResource("/Volumes/Share/hadoop/hbase-1.0.0/conf/hbase-site.xml")
    conf
  }
  val hbconf: Configuration = initHbase()

  //创建HBase的Table
  def createTable(tableName: String, cfname: String): Unit = {
    try {
      val admin = new HBaseAdmin(hbconf)
      if (!admin.isTableAvailable(tableName)) {
        println("###Create Table " + tableName)
        val tableDesc = new HTableDescriptor(tableName)
        val colDesc = new HColumnDescriptor(Bytes.toBytes(cfname))
        colDesc.setCompressionType(org.apache.hadoop.hbase.io.compress.Compression.Algorithm.GZ)
        tableDesc.addFamily(colDesc)
        admin.createTable(tableDesc)
      }
    } catch {
      case e:Throwable => println("@@@createTable Error: "+e.getClass.getName+": "+e.getMessage)
    }
  }

  //删除HBase的Table
  def dropTable(tableName : String): Unit = {
    try {
      val admin = new HBaseAdmin(hbconf)
      if (admin.isTableAvailable(tableName)) {
        if (!admin.isTableDisabled(tableName)) admin.disableTable(tableName)
        admin.deleteTable(tableName)
        println("###Drop table " + tableName)
      } else {
        println("###No table " + tableName + " to drop")
      }
    } catch {
      case e:Throwable => println("@@@dropTable Error: "+e.getClass.getName+": "+e.getMessage)
    }
  }

  //truncate HBase Table
  def truncateTable(tableName: String): Unit = {
    try {
      val admin = new HBaseAdmin(hbconf)
      if (admin.isTableAvailable(tableName)) {
        val td = admin.getTableDescriptor(Bytes.toBytes(tableName))
        if (!admin.isTableDisabled(tableName)) admin.disableTable(tableName)
        admin.deleteTable(tableName)
        admin.createTable(td)
        println("###Truncate table " + tableName)
      } else {
        println("###No table " + tableName + " to truncate")
      }
    } catch {
      case e:Throwable => println("@@@truncateTable Error: "+e.getClass.getName+": "+e.getMessage)
    }
  }

  //删除HBase特定行
  def deleteRowkey(tableName: String, rowkey: String) : Unit = {
    try {
      val table = new HTable(hbconf, tableName)
      val delete = new Delete(Bytes.toBytes(rowkey))
      table.delete(delete)
    } catch {
      case e:Throwable => println("@@@deleteRowkey Error: "+e.getClass.getName+": "+e.getMessage)
    }
  }

  def scanTableLocal(tableName: String, cfName: String, notStrCols: Map[String, String], keyCol: String = "", count: Int = 0): HashMap[String, Map[String, Any]] = {
    val ret = HashMap[String, Map[String, Any]]()
    try {
      val table = new HTable(hbconf, tableName)
      val scan = new Scan()
      scan.addFamily(Bytes.toBytes(cfName))
      if (count > 0) {
        val filter = new PageFilter(count)
        scan.setFilter(filter)
      }
      val result = table.getScanner(scan)
      val ri = result.iterator()
      while (ri.hasNext) {
        val m = Map[String, Any]()
        val rs: Result = ri.next()
        val rowkey = Bytes.toString(rs.getRow())
        for (kv <- rs.raw()) {
          val colName = Bytes.toString(kv.getQualifier)
          if (notStrCols.contains(colName)) {
            notStrCols(colName).toLowerCase() match {
              case "int" => m(colName) = Bytes.toInt(kv.getValue)
              case "long" => m(colName) = Bytes.toLong(kv.getValue)
              case "float" => m(colName) = Bytes.toFloat(kv.getValue)
              case _ => m(colName) = Bytes.toString(kv.getValue)
            }
          } else {
            m(colName) = Bytes.toString(kv.getValue)
          }
        }
        if (keyCol != "" && m.contains(keyCol)) {
          ret(m(keyCol).asInstanceOf[String]) = m
        } else {
          ret(rowkey) = m
        }
      }
    } catch {
      case e:Throwable => println("@@@scanTableLocal Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  def getTableLocal(tableName: String, rowkey: String, cfName: String, notStrCols: Map[String, String]): Map[String, Any] ={
    val ret = Map[String, Any]()
    try {
      val table = new HTable(hbconf, tableName)
      val get = new Get(Bytes.toBytes(rowkey))
      get.addFamily(Bytes.toBytes(cfName))
      val hbaseRDD = table.get(get)
      for (kv <- hbaseRDD.raw) {
        val colName = Bytes.toString(kv.getQualifier)
        if (notStrCols.contains(colName)) {
          notStrCols(colName).toLowerCase() match {
            case "int" => ret(colName) = Bytes.toInt(kv.getValue)
            case "long" => ret(colName) = Bytes.toLong(kv.getValue)
            case "float" => ret(colName) = Bytes.toFloat(kv.getValue)
            case _ => ret(colName) = Bytes.toString(kv.getValue)
          }
        } else {
          ret(colName) = Bytes.toString(kv.getValue)
        }
      }
    } catch {
      case e:Throwable => println("@@@getTableLocal Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //获取递增值
  def getIncr(incrKey: String) : Long = {
    var ret = 0L
    try {
      val tableName = "urls_incr"
      val cfName = "info"
      val colName = "incrvalue"
      val table = new HTable(hbconf, tableName)
      var valMap = Map[String, Any]()
      valMap = Map(colName -> 0L)
      checkRowkey(tableName, incrKey, cfName, valMap)
      val incr = table.incrementColumnValue(Bytes.toBytes(incrKey), Bytes.toBytes(cfName), Bytes.toBytes(colName), 1) //计数器加1
      ret = incr
    } catch {
      case e:Throwable => println("@@@getIncr Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //列值进行递增
  def incrValue(tableName: String, rowkey: String, cfName: String, colName: String) : Long = {
    var ret = 0L
    try {
      val table = new HTable(hbconf, tableName)
      val incr = table.incrementColumnValue(Bytes.toBytes(rowkey), Bytes.toBytes(cfName), Bytes.toBytes(colName), 1L)
      ret = incr
    } catch {
      case e:Throwable => println("@@@incrValue Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //检查行健是否重复并进行插入操作
  def checkRowkey(tableName: String, rowkey: String, cfName: String, valMap: Map[String, Any], replace: Boolean = false) : Int = {
    var ret = 0
    try {
      val table = new HTable(hbconf, tableName)
      val get = new Get(Bytes.toBytes(rowkey))
      val hbaseRDD = table.get(get)
      for (kv <- hbaseRDD.raw) {
        ret = 1
      }
      if (ret == 0 || replace) {
        table.setAutoFlush(false)
        val put = new Put(Bytes.toBytes(rowkey))
        for ((k, v) <- valMap) {
          if (v.isInstanceOf[Int]) put.add(Bytes.toBytes(cfName), Bytes.toBytes(k), Bytes.toBytes(v.asInstanceOf[Int]))
          if (v.isInstanceOf[String]) put.add(Bytes.toBytes(cfName), Bytes.toBytes(k), Bytes.toBytes(v.asInstanceOf[String]))
          if (v.isInstanceOf[Long]) put.add(Bytes.toBytes(cfName), Bytes.toBytes(k), Bytes.toBytes(v.asInstanceOf[Long]))
          if (v.isInstanceOf[Float]) put.add(Bytes.toBytes(cfName), Bytes.toBytes(k), Bytes.toBytes(v.asInstanceOf[Float]))
        }
        table.put(put)
        table.flushCommits()
      }
    } catch {
      case e:Throwable => println("@@@checkRowkey Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //检查行健是否重复，并插入递增的id列值
  def checkIdRowkey(tableName: String, rowkey: String, cfName: String, idCol: String, valMap: Map[String, Any], replace: Boolean = false) : Long = {
    var ret = 0L
    try {
      val table = new HTable(hbconf, tableName)
      val get = new Get(Bytes.toBytes(rowkey))
      val hbaseRDD = table.get(get)
      for (kv <- hbaseRDD.raw) {
        if (Bytes.toString(kv.getFamily) == cfName && Bytes.toString(kv.getQualifier) == idCol) ret = Bytes.toLong(kv.getValue())
      }
      if (ret == 0L || replace) {
        table.setAutoFlush(false)
        val id = getIncr(tableName)
        val put = new Put(Bytes.toBytes(rowkey))
        put.add(Bytes.toBytes(cfName), Bytes.toBytes(idCol), Bytes.toBytes(id))
        for ((k, v) <- valMap) {
          if (v.isInstanceOf[Int]) put.add(Bytes.toBytes(cfName), Bytes.toBytes(k), Bytes.toBytes(v.asInstanceOf[Int]))
          if (v.isInstanceOf[String]) put.add(Bytes.toBytes(cfName), Bytes.toBytes(k), Bytes.toBytes(v.asInstanceOf[String]))
          if (v.isInstanceOf[Long]) put.add(Bytes.toBytes(cfName), Bytes.toBytes(k), Bytes.toBytes(v.asInstanceOf[Long]))
          if (v.isInstanceOf[Float]) put.add(Bytes.toBytes(cfName), Bytes.toBytes(k), Bytes.toBytes(v.asInstanceOf[Float]))
        }
        table.put(put)
        table.flushCommits()
      }
    } catch {
      case e:Throwable => println("@@@checkIdRowkey Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //获取sogou词库的词库url列表，并入库
  def parseSogouDicUrlToHbase(url: String): Unit = {
    try {
      val arr = url.replaceAll("http://pinyin.sogou.com/dict/cate/index/", "").split("/download/")
      val cfName = "info"
      val dicCategoryId = arr(0).toInt
      val currentPage = arr(1).toInt
      val sogouDicList = mapSogouDicList(dicCategoryId, currentPage, true)
      val dicList = sogouDicList("dics").asInstanceOf[ArrayBuffer[(String, String, String)]]
      val category = sogouDicList("category").asInstanceOf[String]
      val keyValMap = ArrayBuffer[Map[String, Map[String, String]]]()
      dicList.foreach { s => {
        val dicUrl = "http://pinyin.sogou.com/dict/download_txt.php?id=" + s._1
        val dicid = s._1
        val name = s._2
        val count = s._3
        val dicUrlRowkey = md5(dicUrl)
        var valMap = Map[String, Any]()
        valMap = Map(
          "url" -> dicUrl
        )
        checkRowkey("urls_dictterm", dicUrlRowkey, cfName, valMap)
        keyValMap += Map(dicid -> Map("name" -> name, "count" -> count, "category" -> category))

      }}
      var urlRowkey = md5(url)
      deleteRowkey("urls_dictlist", urlRowkey)
      checkTableKeyBatch("dics_list", "dicid", "dicid", "", keyValMap)
    } catch {
      case e:Throwable => println("@@@parseSogouDicUrlToHbase Error: "+e.getClass.getName+": "+e.getMessage)
    }
  }

  //解释Baike的Url并进行分词入库
  def parseBaikeTermToHbase(url: String) : Unit = {
    try {
      val urlInfo = mapBaikeInfo(url)
      var redirectUrl = urlInfo("redirectUrl").asInstanceOf[String]
      var charset = urlInfo("charset").asInstanceOf[String]
      var html = urlInfo("html").asInstanceOf[String]
      var content = urlInfo("content").asInstanceOf[String]
      var tags = urlInfo("tags").asInstanceOf[String]
      var term = urlInfo("title").asInstanceOf[String]
      var keyword = urlInfo("keyword").asInstanceOf[String]
      var hrefs = urlInfo("hrefs").asInstanceOf[ArrayBuffer[String]]
      var i = 0
      content = (term + " ") * 4 + (tags + " ") * 3 + (keyword + " ") * 2 + content
      if (urlInfo("content").asInstanceOf[String] != "") {
        var cfName = "info"
        var redirectUrlRowkey = md5(redirectUrl)

        var tagids = ""
        if (tags != "") {
          val tagArr = tags.split("，").map(s => s.trim)
          val tagidArr = ArrayBuffer[Long]()
          i = 0
          tagArr.foreach { tag => {
            i = i + 1
            checkTableKeyBatch("proc_term", "term", "termid", "termfreq", ArrayBuffer(Map(tag -> Map[String, String]())))
            val tidArr = checkTableKeyBatch("proc_tags", "tag", "tagid", "tagcount", ArrayBuffer(Map(tag -> Map[String, String]())),false,1000,true)
            val tagid = tidArr(0).toLong
            tagidArr += tagid
            val tagidRowkey = md5(tagid.toString)
            val tagidurlRowkey = tagidRowkey + "-" + redirectUrlRowkey
            val valMap3 = Map(
              "tagid" -> tagid,
              "url" -> redirectUrl
            )
            checkRowkey("urls_baiketagid", tagidurlRowkey, cfName, valMap3, true)
          }
          }
          tagids = tagidArr.mkString(" ")
        }
        println("#####" + redirectUrl + " insert " + i + " tags")

        i = 0
        hrefs.foreach { href => {
          i = i + 1
          val hrefRowkey = md5(href)
          var valMap = Map[String, Any]()
          valMap = Map(
            "url" -> href
          )
          checkRowkey("urls_baiketodo", hrefRowkey, cfName, valMap, true)
          val linkidRowkey = redirectUrlRowkey + "-" + hrefRowkey
          var valMap2 = Map[String, Any]()
          valMap2 = Map(
            "fromurl" -> redirectUrl,
            "tourl" -> href
          )
          checkRowkey("urls_baikelink", linkidRowkey, cfName, valMap2, true)
        }
        }
        println("#####" + redirectUrl + " insert " + i + " hrefs")

        //分词并进行入库
        val termids: String = parseSplitTermToMysql(content)

        //插入跳转url
        var valMap = Map[String, Any]()
        valMap = Map(
          "url" -> redirectUrl,
          "redirectUrl" -> "",
          "charset" -> charset,
          "html" -> html,
          "content" -> content,
          "tags" -> tags,
          "term" -> term,
          "keyword" -> keyword,
          "tagids" -> tagids,
          "termids" -> termids
        )
        val urlid = checkIdRowkey("urls_baikedone", redirectUrlRowkey, cfName, "urlid", valMap)
        deleteRowkey("urls_baiketodo", redirectUrlRowkey)

        //插入源url
        if (url != redirectUrl) {
          var urlRowkey = md5(url)
          deleteRowkey("urls_baiketodo", urlRowkey)
        }
      } else {
        var urlRowkey = md5(url)
        deleteRowkey("urls_baiketodo", urlRowkey)
      }
    } catch {
      case e:Throwable => println("@@@parseBaikeTermToHbase Error: "+e.getClass.getName+": "+e.getMessage)
    }
  }
}

