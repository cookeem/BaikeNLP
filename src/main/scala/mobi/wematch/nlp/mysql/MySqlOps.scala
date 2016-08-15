package mobi.wematch.nlp.mysql

import java.sql.{Connection, DriverManager, Statement}

import mobi.wematch.nlp.hbase.HBaseOps._
import mobi.wematch.nlp.http.HttpOps._
import mobi.wematch.nlp.strops.StrOps._

import scala.collection.mutable.{ArrayBuffer, Map}

/**
 * Created by cookeem on 15/7/26.
 */
object MySqlOps {
  //初始化Mysql连接
  def initMysql(hostname : String, port : Int, dbname : String, username : String, password : String): Connection = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://"+hostname+":"+port+"/"+dbname+"?rewriteBatchedStatements=true&amp;useUnicode=true&amp;characterEncoding=UTF-8"
    var conn: Connection = null
    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(url, username, password)
      conn.setAutoCommit(false)
    } catch {
      case e:Throwable => println("@@@initMysql Error: "+e.getClass.getName+": "+e.getMessage)
    }
    conn
  }
  val mysqlConn = initMysql("localhost",3306,"termdb","root","asdasd")

  //执行语句，返回记录集
  def querySql(sql : String): ArrayBuffer[Map[String, String]] = {
    var ret = ArrayBuffer[Map[String, String]]()
    //获取查询结果
    try {
      val pstmt = mysqlConn.prepareStatement(sql)
      val rs = pstmt.executeQuery(sql)
      while (rs.next()) {
        val map = Map[String,String]()
        for (i <- 1 to rs.getMetaData().getColumnCount) {
          val key = rs.getMetaData().getColumnName(i)
          val value = rs.getString(i)
          map += (key -> value)
        }
        ret += map
      }
      rs.close()
      pstmt.close()
    } catch {
      case e:Throwable => println("@@@querySql Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //执行语句，无返回记录集
  def execSql(sql : String): Int = {
    var ret = 0
    //获取查询结果
    try {
      val pstmt = mysqlConn.prepareStatement(sql)
      ret = pstmt.executeUpdate(sql)
      mysqlConn.commit()
      pstmt.close()
    } catch {
      case e:Throwable => println("@@@execSql Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //插入记录
  def insertTable(tableName: String, valMap: Map[String, String]): Int = {
    var ret = 0
    try {
      val colsArr = ArrayBuffer[String]()
      val valsArr,tvsArr = ArrayBuffer[String]()
      for ((k,v) <- valMap) {
        colsArr += k
        valsArr += v
        tvsArr += "?"
      }
      val cols = colsArr.mkString(",")
      var vals = valsArr.mkString(",")
      var tvs = tvsArr.mkString(",")
      val sql = "INSERT INTO "+tableName+"("+cols+") VALUES ("+tvs+")"
      val pstmt = mysqlConn.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS)
      var i = 0
      valsArr.foreach(v => {
        i = i + 1
        pstmt.setString(i,v)
      })
      pstmt.executeUpdate()
      mysqlConn.commit()
      val rs = pstmt.getGeneratedKeys()
      if (rs.next()) ret = rs.getInt(1)
      rs.close
      pstmt.close()
    } catch {
      case e:Throwable => println("@@@insertTable Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //更新表中的特定Where记录
  def updateTable(tableName: String, valMap: Map[String, String], whereMap: Map[String, String]): Unit = {
    var ret = 0
    try {
      val cvsArr,cvsArr2 = ArrayBuffer[String]()
      val wvsArr,wvsArr2 = ArrayBuffer[String]()
      for ((k,v) <- valMap) {
        cvsArr += k+"=?"
        cvsArr2 += v
      }
      for ((k,v) <- whereMap) {
        wvsArr += "AND "+k+"=?"
        wvsArr2 += v
      }
      val cvs = cvsArr.mkString(",")
      val wvs = wvsArr.mkString(" ")
      val sql = "UPDATE "+tableName+" SET "+cvs+" WHERE 1 = 1 "+wvs
      val pstmt = mysqlConn.prepareStatement(sql)
      var i = 0
      cvsArr2.foreach(v => {
        i = i + 1
        pstmt.setString(i,v)
      })
      wvsArr2.foreach(v => {
        i = i + 1
        pstmt.setString(i,v)
      })
      ret = pstmt.executeUpdate()
      mysqlConn.commit()
      pstmt.close()
    } catch {
      case e:Throwable => println("@@@updateTable Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //简单的elect Where语句查询
  def selectSimpleTable(tableName: String, whereMap: Map[String, String] = Map[String, String](), selectMap: Set[String] = Set("*")): ArrayBuffer[Map[String, String]] ={
    var ret = ArrayBuffer[Map[String, String]]()
    try {
      val wvsArr,wvsArr2 = ArrayBuffer[String]()
      val selectStr = selectMap.mkString(",")
      for ((k,v) <- whereMap) {
        wvsArr += "AND "+k+"=?"
        wvsArr2 += v
      }
      val wvs = wvsArr.mkString(" ")
      val sql = "SELECT "+selectStr+" FROM "+tableName+" WHERE 1 = 1 "+wvs
      val pstmt = mysqlConn.prepareStatement(sql)
      var i = 0
      wvsArr2.foreach(v => {
        i = i + 1
        pstmt.setString(i,v)
      })
      val rs = pstmt.executeQuery()
      var j = 0
      while (rs.next()) {
        j = j + 1
        val map = Map[String,String]()
        for (i <- 1 to rs.getMetaData().getColumnCount) {
          val key = rs.getMetaData().getColumnName(i)
          val value = rs.getString(i)
          map += key -> value
        }
        ret += map
      }
      rs.close()
      pstmt.close()
    } catch {
      case e:Throwable => println("@@@selectSimpleTable Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //检查关键字是否存在，不存在则插入数据
  def checkTableKey(tableName: String, keyCol: String, idCol: String, incrCol: String, keyVal: String, valMap: Map[String, String]): Int = {
    var ret = 0
    try {
      var rs = selectSimpleTable(tableName,Map(keyCol -> keyVal),Set(idCol))
      var valMap2 = Map(keyCol -> keyVal) ++ valMap
      if (rs.length > 0) {
        ret = rs(0)(idCol).toInt
        if (valMap != Map[String, String]()) {
          updateTable(tableName, valMap, Map(idCol -> ret.toString))
        }
        val sql = "UPDATE " + tableName + " SET " + incrCol + "=" + incrCol + "+1 WHERE " + idCol + "=" + ret
        execSql(sql)
      } else {
        ret = insertTable(tableName, valMap2)
      }
    } catch {
      case e:Throwable => println("@@@checkTableKey Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //获取统计数量
  def getCountTable(tableName: String, countCol: String, whereMap: Map[String, String]) : Int = {
    var ret = 0
    try {
      val whereColValArr = ArrayBuffer[String]()
      var whereColValStr = ""
      for ((k, v) <- whereMap) {
        whereColValArr += k + "=?"
      }
      whereColValStr = whereColValArr.mkString(" AND ")
      var sql = "SELECT COUNT(" + countCol + ") FROM " + tableName
      if (whereColValStr != "") sql = sql + " WHERE " + whereColValStr
      val pstmt = mysqlConn.prepareStatement(sql)
      var i = 0
      for ((k, v) <- whereMap) {
        i = i + 1
        pstmt.setString(i, v)
      }
      val rs = pstmt.executeQuery()
      rs.next()
      ret = rs.getInt(1)
      rs.close()
      pstmt.close()
    } catch {
      case e:Throwable => println("@@@getCountTable Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //批量检查关键字是否存在，不存在则插入数据，并进行计数递增
  //batch中内容不能重复
  //keyValMap中不能包含keyCol的数值Map
  def checkTableKeyBatch(tableName: String, keyCol: String, idCol: String, incrCol: String, keyValMap: ArrayBuffer[Map[String, Map[String, String]]], batchMode: Boolean = false, batchsize: Int = 1000, returnIds: Boolean = false): ArrayBuffer[Int] = {
    val ret = ArrayBuffer[Int]()
    var j = 0
    var kvm = Map[String, Map[String, String]]()
    if (keyValMap.length > 0) {
      kvm = keyValMap(0)
    }
    var t1 = System.currentTimeMillis
    try {
      val updateValArr = ArrayBuffer[String](keyCol+"=?")
      val updateColArr = ArrayBuffer[String](keyCol)
      var updateValStr = ""
      val insertColArr = ArrayBuffer[String](keyCol)
      val insertValArr = ArrayBuffer[String]("?")
      var insertColStr = ""
      var insertValStr = ""
      for ((key, value) <- kvm) {
        for ((k, v) <- value) {
          if (insertColArr.contains(k) == false) {
            insertColArr += k
            insertValArr += "?"
          }
          if (updateValArr.contains(k) == false) {
            updateColArr += k
            updateValArr += k + "=?"
          }
        }
      }
      updateValStr = updateValArr.mkString(",")
      insertColStr = insertColArr.mkString(",")
      insertValStr = insertValArr.mkString(",")
      val sqlUpdate = "UPDATE " + tableName + " SET " + updateValStr + " WHERE " + keyCol + " = ? "
      val pstmtUpdate = mysqlConn.prepareStatement(sqlUpdate)
      val sqlIncr = "UPDATE " + tableName + " SET " + incrCol + "=" + incrCol + "+1 WHERE " + keyCol + " = ? "
      val pstmtIncr = mysqlConn.prepareStatement(sqlIncr)
      val sqlInsert = "INSERT INTO " + tableName + "(" + insertColStr + ") VALUES (" + insertValStr + ")"
      val pstmtInsert = mysqlConn.prepareStatement(sqlInsert)
      keyValMap.foreach(m => {
        j = j + 1
        for ((key, value) <- m) {
          var findkey = getCountTable(tableName, keyCol, Map(keyCol -> key))
          if (findkey > 0) {
            var i = 0
            i = i + 1
            pstmtUpdate.setString(i, key)
            for ((k, v) <- value) {
              if (updateColArr.contains(k) == true) {
                i = i + 1
                pstmtUpdate.setString(i, v)
              }
            }
            i = i + 1
            pstmtUpdate.setString(i, key)
            pstmtUpdate.addBatch()
            if (incrCol != "") {
              pstmtIncr.setString(1, key)
              pstmtIncr.addBatch()
            }
            pstmtUpdate.executeBatch()
          } else {
            var i = 0
            i = i + 1
            pstmtInsert.setString(i, key)
            for ((k, v) <- value) {
              if (insertColArr.contains(k) == true) {
                i = i + 1
                pstmtInsert.setString(i, v)
              }
            }
            pstmtInsert.addBatch()
            if (incrCol != "") {
              pstmtIncr.setString(1, key)
              pstmtIncr.addBatch()
            }
            pstmtInsert.executeBatch()
          }
        }
        if (incrCol != "") {
          pstmtIncr.executeBatch()
        }
        if (batchMode == false) {
          mysqlConn.commit()
          pstmtUpdate.clearParameters()
          pstmtIncr.clearParameters()
          pstmtInsert.clearParameters()
          pstmtUpdate.clearBatch()
          pstmtIncr.clearBatch()
          pstmtInsert.clearBatch()
        }
        if ((j % batchsize) == 0) {
          if (batchMode == true) {
            mysqlConn.commit()
            pstmtUpdate.clearParameters()
            pstmtIncr.clearParameters()
            pstmtInsert.clearParameters()
            pstmtUpdate.clearBatch()
            pstmtIncr.clearBatch()
            pstmtInsert.clearBatch()
          }
          val t2 = System.currentTimeMillis
          val ts = Math.round((t2 - t1) * 100) / (100 * 1000.00)
          println("###insert the "+j+"th record with batchsize " + batchsize + " to mysql in " + ts + " seconds")
          t1 = System.currentTimeMillis
        }
      })
      pstmtUpdate.close()
      pstmtIncr.close()
      pstmtInsert.close()

      val t2 = System.currentTimeMillis
      var ts = Math.round((t2 - t1) * 100) / (100 * 1000.00)
      println("###insert the "+j+"th record with batchsize " + batchsize + " to mysql in " + ts + " seconds")
      if (returnIds) {
        keyValMap.foreach(m => {
          j = j + 1
          for ((key, value) <- m) {
            val idarr = selectSimpleTable(tableName, Map(keyCol -> key), Set(idCol))
            var id = 0
            if (idarr.length > 0) id = idarr(0)(idCol).toInt
            ret += id
          }
        })
        ts = Math.round((System.currentTimeMillis - t2) * 100) / (100 * 1000.00)
        println("###get "+tableName+" ids in " + ts + " seconds")
      }
    } catch {
      case e:Throwable => println("@@@checkTableKeyBatch Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //获取sogou词库的词汇，并入库
  def parseSogouDicTermToMysql(url: String): Unit = {
    try {
      val dicid = url.replaceAll("http://pinyin.sogou.com/dict/download_txt.php\\?id=", "").toInt
      var terms = mapSogouDicInfo(dicid)("terms").asInstanceOf[Array[String]]
      val keyValMap = ArrayBuffer[Map[String, Map[String, String]]]()
      val t1 = System.currentTimeMillis
      terms.foreach(t => {
        var term = ""
        if (t.replaceAll("[a-z,A-Z]", "") != "") {
          term = t.replaceAll("[a-z,A-Z]", "")
        } else {
          term = t
        }
        if (term.getBytes().length < 17 && term != "") {
          keyValMap += Map(new String(term.getBytes(), "UTF-8") -> Map("dicid" -> dicid.toString))
        }
      })
      val t2 = System.currentTimeMillis
      val ts = Math.round((t2 - t1) * 100) / (100 * 1000.00)
      println("###put terms to keyvalmap in " + ts + " seconds")
      checkTableKeyBatch("dics_term", "term", "termid", "termfreq", keyValMap)
      terms = null
      deleteRowkey("urls_dictterm", md5(url))
    } catch {
      case e:Throwable => println("@@@parseSogouDicTermToMysql Error: "+e.getClass.getName+": "+e.getMessage)
    }
  }

  //进行ansj分词并入库mysql，返回termid的String
  def parseSplitTermToMysql(content: String): String ={
    var ret = ""
    try {
      val t1 = System.currentTimeMillis
      val keyValMap = splitChinese(content)
      val cfName = "info"
      val ts = Math.round((System.currentTimeMillis - t1) * 100) / (100 * 1000.00)
      println("###split content in " + keyValMap.length + " terms to keyvalmap in " + ts + " seconds")
      val ida = checkTableKeyBatch("proc_term", "term", "termid", "termfreq", keyValMap, false, 1000, true)
      println("###ansj split " + keyValMap.length + " words in " + ts + " seconds")
      ret = ida.mkString(" ")
    } catch {
      case e:Throwable => println("@@@parseSplitTermToMysql Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //从词汇表获取所有有效词汇
  def getTermsFromMysql(page: Int = 0, count: Int = 1000): ArrayBuffer[Map[String, String]] = {
    var ret = ArrayBuffer[Map[String, String]]()
    try {
      var sql = "SELECT t.* FROM dics_term t,dics_list d WHERE t.dicid=d.dicid AND d.count>0 ORDER BY termid LIMIT " + (page - 1) * count + "," + count
      val termsArr = querySql(sql)
      ret = termsArr
    } catch {
      case e:Throwable => println("@@@getTermsFromMysql Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //获取词汇表最大的单词ID
  def getTermMaxId(): Int = {
    var ret = 0
    try {
      val sql = "SELECT MAX(t.termid) maxid FROM dics_term t,dics_list d WHERE t.dicid=d.dicid AND d.count>0"
      val maxArr = querySql(sql)
      if (maxArr.length > 0) ret = maxArr(0)("maxid").toInt
    } catch {
      case e:Throwable => println("@@@getTermMaxId Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }

  //获取词汇表最后的单词
  def getTermLast(): String = {
    var ret = ""
    try {
      var sql = "SELECT t.term FROM dics_term t,dics_list d WHERE t.dicid=d.dicid AND d.count>0 ORDER BY termid DESC LIMIT 1"
      val termsArr = querySql(sql)
      if (termsArr.length > 0) ret = termsArr(0)("term")
    } catch {
      case e:Throwable => println("@@@getTermLast Error: "+e.getClass.getName+": "+e.getMessage)
    }
    ret
  }
}
