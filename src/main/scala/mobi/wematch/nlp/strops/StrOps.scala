package mobi.wematch.nlp.strops

import java.security.MessageDigest

import mobi.wematch.nlp.mysql.MySqlOps._
import org.ansj.library.UserDefineLibrary
import org.ansj.recognition.NatureRecognition
import org.ansj.splitWord.analysis.ToAnalysis

import scala.collection.mutable.{ArrayBuffer, Map}

/**
 * Created by cookeem on 15/7/25.
 */
object StrOps {
  //md5字符串
  def md5(s: String): String= {
    val messageDigest = MessageDigest.getInstance("MD5")
    messageDigest.update(s.getBytes())
    val buffer = messageDigest.digest()
    val sb = new StringBuffer(buffer.length * 2)
    buffer.foreach(b => {
      sb.append(Character.forDigit((b & 240) >> 4, 16))
      sb.append(Character.forDigit((b & 15), 16))
    })
    sb.toString
  }

  //字符串固定长度填充特定字符
  def fillString(s: String, len: Int, ch: String, left: Int): String = {
    var slen = s.getBytes().length
    var str = s
    if (slen < len) {
      while (slen < len) {
        if (left == 1) {
          str = ch + str
        } else {
          str = str + ch
        }
        slen = slen + 1
      }
    }
    str
  }

  def loadAnsjLib(): Unit = {
    var t1 = System.currentTimeMillis
    var word1 = ""
    var word2 = ""
    val libPath = "/Volumes/Share/hadoop/publiclib/library"
    val libFile = libPath + "/default.dic"
    try {
      word1 = scala.io.Source.fromFile(libFile).mkString.split("\n").last.split("\t")(0)
      word2 = getTermLast()
      if (UserDefineLibrary.contains(word1) == false) {
      try {
        println("###Begin to load ansj library")
        UserDefineLibrary.loadLibrary(UserDefineLibrary.FOREST, libPath)
        val ts = Math.round((System.currentTimeMillis - t1) * 100) / (100 * 1000.00)
        println("###Finish load ansj library in "+ts+" seconds")
        var page = 1
        val count = 100000
        var j = 0
        val t2 = System.currentTimeMillis
        var finish = 0
        while (finish == 0 && j < 1500000) {
          val t1 = System.currentTimeMillis
          println("###Begin to load terms to ansj library")
          var termsArr = getTermsFromMysql(page, count)
          var tmaxid = 0
          termsArr.foreach(m => {
            val term = m("term")
            var termtype = "nwt"
            val termid = m("termid").toInt
            val termfreq = m("termfreq").toInt + 1000
            if (UserDefineLibrary.contains(term) == false) {
              UserDefineLibrary.insertWord(term, termtype, termfreq)
            }
            if (termid > tmaxid) tmaxid = termid
            j = j + 1
          })
          if (termsArr == ArrayBuffer[Map[String, String]]()) finish = 1
          termsArr = ArrayBuffer[Map[String, String]]()
          page = page + 1
          val ts = Math.round((System.currentTimeMillis - t1) * 100) / (100 * 1000.00)
          println("###Finish load " + j + "th/" + count + " terms (termid: " + tmaxid + ") to ansj library in " + ts + " seconds")
        }
        val ts2 = Math.round((System.currentTimeMillis - t2) * 100) / (100 * 1000.00)
        println("###Finish load all "+j+" terms to ansj library in "+ts2+" seconds")
      } catch {
        case e:Throwable => {
          println("loadAnsjLib@@@Error: "+e.getClass.getName+": "+e.getMessage)
          val waitTime = 10000L
          println("###Ansj load lib error, begin to wait "+waitTime/1000+" seconds")
          var j = 0
          do {
            j = j + 1
            Thread.sleep(waitTime)
            println("###Wait ansj to load lib for "+waitTime/1000+" seconds")
            println("###Try to get UserDefineLibrary.contains(\""+word1+"\"):"+UserDefineLibrary.contains(word1))
            println("###Try to get UserDefineLibrary.contains(\""+word2+"\"):"+UserDefineLibrary.contains(word2))
          }
          while (!(UserDefineLibrary.contains(word1) && UserDefineLibrary.contains(word2)) && (j < 6))
          println("###Finish wait ansj load lib for "+waitTime/1000+" seconds")
          println("###Finish UserDefineLibrary.contains(\""+word1+"\"):"+UserDefineLibrary.contains(word1))
          println("###Finish UserDefineLibrary.contains(\""+word2+"\"):"+UserDefineLibrary.contains(word2))
          if (!(UserDefineLibrary.contains(word1) && UserDefineLibrary.contains(word2))) {
            loadAnsjLib()
          }
        }
       }
    }
    } catch {
      case e:Throwable => println("@@@Error: "+e.getClass.getName+": "+e.getMessage)
    }
  }

  //Ansj中文分词，进行词性识别以及过滤停用词
  def splitChinese(content: String) : ArrayBuffer[Map[String,Map[String,String]]] = {
    val t1 = System.currentTimeMillis
    var wa = ArrayBuffer[Map[String,Map[String,String]]]()
    try {
      var ws = ToAnalysis.parse(content)
      val ts = Math.round((System.currentTimeMillis - t1) * 100) / (100 * 1000.00)
      println("###Finish split content in " + ts + " seconds")
      new NatureRecognition(ws).recognition()
      val wi = ws.iterator
      while (wi.hasNext()) {
        val t = wi.next()
        if (t.getName.trim == "" || t.getName.trim == "是" || t.getName.trim == "的" || t.getName.trim == "得" || t.getName.trim == "了" || t.getNatureStr == "null" || t.getNatureStr == "vshi" || t.getNatureStr == "vyou" || t.getNatureStr.startsWith("t") || t.getNatureStr.startsWith("f") || t.getNatureStr.startsWith("r") || t.getNatureStr.startsWith("m") || t.getNatureStr.startsWith("q") || t.getNatureStr.startsWith("d") || t.getNatureStr.startsWith("p") || t.getNatureStr.startsWith("c") || t.getNatureStr.startsWith("u") || t.getNatureStr.startsWith("e") || t.getNatureStr.startsWith("y") || t.getNatureStr.startsWith("o") || t.getNatureStr.startsWith("h") || t.getNatureStr.startsWith("k") || t.getNatureStr.startsWith("x") || t.getNatureStr.startsWith("w")) {
          wi.remove()
        } else {
          wa += Map(t.getName -> Map("termtype" -> t.getNatureStr))
        }
      }
    } catch {
      case e:Throwable => println("@@@Error: "+e.getClass.getName+": "+e.getMessage)
    }
    wa
  }
}
