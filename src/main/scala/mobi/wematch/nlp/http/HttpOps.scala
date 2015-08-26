package mobi.wematch.nlp.http

import java.io.ByteArrayOutputStream
import java.net.{HttpURLConnection, InetSocketAddress, Proxy, URL}

import info.monitorenter.cpdetector.io._
import org.jsoup.Jsoup
import org.nlpcn.commons.lang.jianfan.JianFan._

import scala.collection.mutable.{ArrayBuffer, Map}

/**
 * Created by cookeem on 15/7/29.
 */
object HttpOps {
  //获取url的编码、跳转连接、正文
  def mapUrlInfo(url: String) : Map[String,String] = {
    println("###Begin to get : "+url)
    val t1 = System.currentTimeMillis
    val detector = CodepageDetectorProxy.getInstance()
    detector.add(new ParsingDetector(false))
    detector.add(new ByteOrderMarkDetector())
    detector.add(ASCIIDetector.getInstance())
    detector.add(UnicodeDetector.getInstance())
    detector.add(JChardetFacade.getInstance())
    var ret = Map(
      "redirectUrl" -> "",
      "charset" -> "",
      "html" -> ""
    )
    var conn: HttpURLConnection = null
    try {
      val proxyHost = System.getProperty("http.proxyHost")
      val proxyPort = System.getProperty("http.proxyPort")
      if (proxyHost == null) {
        conn = (new URL(url)).openConnection().asInstanceOf[HttpURLConnection]
      } else {
        val proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort.toInt))
        conn = (new URL(url)).openConnection(proxy).asInstanceOf[HttpURLConnection]
      }
      conn.setRequestMethod("GET")
      conn.setInstanceFollowRedirects(false)
      conn.setConnectTimeout(30000)
      conn.setReadTimeout(30000)
      conn.connect()
      val location = conn.getHeaderField("Location")
      var redirectUrl = url
      if (location != null) {
        redirectUrl = location
      }
      val charset = detector.detectCodepage(new URL(redirectUrl)).toString
      if (redirectUrl != url) {
        conn.disconnect()
        if (proxyHost == null) {
          conn = (new URL(redirectUrl)).openConnection().asInstanceOf[HttpURLConnection]
        } else {
          val proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort.toInt))
          conn = (new URL(redirectUrl)).openConnection(proxy).asInstanceOf[HttpURLConnection]
        }
        conn.setRequestMethod("GET")
        conn.setInstanceFollowRedirects(false)
        conn.setConnectTimeout(30000)
        conn.setReadTimeout(30000)
        conn.connect()
      }
      val is = conn.getInputStream()
      var bos = new ByteArrayOutputStream()
      var result = 0
      var i = 0
      do {
        i = i + 1
        result = is.read()
        if (result != -1) bos.write(result.asInstanceOf[Byte])
      } while (result != -1)
      bos.write(0.asInstanceOf[Byte])
      val html = new String(bos.toByteArray(), charset)
      bos.close()
      is.close()
      ret = Map(
        "redirectUrl" -> redirectUrl,
        "charset" -> charset,
        "html" -> html
      )
    } catch {
      case e:Throwable => println("@@@mapUrlInfo Error: "+e.getClass.getName+": "+e.getMessage)
    } finally {
      conn.disconnect()
    }
    val t2 = System.currentTimeMillis
    val ts = Math.round((t2 - t1) * 100) / (100 * 1000.00)
    println("###Finish get "+url+" in "+ts+" seconds")
    ret
  }

  //获取百度百科的解析信息
  def mapBaikeInfo(url : String) : Map[String, Any] = {
    val urlInfo = mapUrlInfo(url)
    var ret = Map[String, Any]()
    ret = Map(
      "url" -> "",
      "redirectUrl" -> "",
      "charset" -> "",
      "html" -> "",
      "content" -> "",
      "tags" -> "",
      "title" -> "",
      "keyword" -> "",
      "hrefs" -> ArrayBuffer[String]()
    )
    if (urlInfo("html") != "") {
      try {
        val charset = urlInfo("charset")
        val html = urlInfo("html")
        val redirectUrl = urlInfo("redirectUrl").split("\\?")(0).split("#")(0)
        val regEx_script = "(?i)<[\\s]*?script[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?script[\\s]*?>"
        val regEx_style = "(?i)<[\\s]*?style[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?style[\\s]*?>"
        val doc = Jsoup.parse(html.replaceAll(regEx_script, "").replaceAll(regEx_style, ""))
        doc.select("script").remove()
        doc.select("style").remove()
        var content = ""
        if (doc.getElementsByClass("body-wrapper") != null) {
          content = doc.getElementsByClass("body-wrapper").text()
        }
        if (doc.getElementById("sec-content0") != null) {
          content = doc.getElementById("sec-content0").text()
        }
        if (content != "") {
          val hrefs = ArrayBuffer[String]()
          if (doc.select("a") != null) {
            val iter = doc.select("a").iterator()
            while (iter.hasNext) {
              val a = iter.next()
              var href = a.attributes.get("href")
              if (href.indexOf("http://baike.baidu.com/view/") > -1 || href.indexOf("http://baike.baidu.com/subview/") > -1) {
                if (href.indexOf("?") > -1) href = href.substring(0, href.indexOf("?"))
                if (href.indexOf("#") > -1) href = href.substring(0, href.indexOf("#"))
                hrefs += href
              }
            }
          }
          var tags = ""
          if (doc.getElementById("open-tag-item") != null) tags = doc.getElementById("open-tag-item").text()
          var title = ""
          if (doc.getElementsByTag("title") != null) title = doc.getElementsByTag("title").text().replaceAll("_百度百科", "")
          var keyword = ""
          if (doc.getElementsByAttributeValue("name", "Keywords") != null) keyword = doc.getElementsByAttributeValue("name", "Keywords").attr("content")
          ret = Map(
            "url" -> url,
            "redirectUrl" -> redirectUrl,
            "charset" -> charset,
            "html" -> html,
            "content" -> content,
            "tags" -> tags,
            "title" -> title,
            "keyword" -> keyword,
            "hrefs" -> hrefs
          )
          println("### Baike title: " + title)
        }
      } catch {
        case e:Throwable => println("@@@mapBaikeInfo Error: "+e.getClass.getName+": "+e.getMessage)
      }
    }
    ret
  }

  //获取搜狗词库列表
  def mapSogouDicList(dicCategoryId: Int, currentPage: Int, getPages : Boolean) : Map[String, Any]= {
    val url = "http://pinyin.sogou.com/dict/cate/index/"+dicCategoryId+"/download/"+currentPage
    val urlInfo = mapUrlInfo(url)
    var ret = Map[String, Any]()
    ret = Map(
      "category" -> "",
      "dics" -> ArrayBuffer[(String, String, String)]()
    )
    if (urlInfo("html") != "") {
      try {
        val charset = urlInfo("charset")
        val html = f2J(urlInfo("html"))
        var regEx_script = "(?i)<[\\s]*?script[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?script[\\s]*?>"
        val regEx_style = "(?i)<[\\s]*?style[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?style[\\s]*?>"
        var doc = Jsoup.parse(html.replaceAll(regEx_script, "").replaceAll(regEx_style, ""))
        doc.select("script").remove()
        doc.select("style").remove()
        var category = doc.select("title").text().split("_")(0)
        val dics = ArrayBuffer[(String, String, String)]()
        //val dicElms = doc.getElementById("dict_detail_list").getElementsByClass("detail_title").select("a")
        val dicElms = doc.getElementsByClass("dict_detail_block")
        if (dicElms != null) {
          var iter = dicElms.iterator()
          while (iter.hasNext) {
            val e = iter.next()
            val href = e.getElementsByClass("detail_title").select("a")
            val title = e.getElementsByClass("detail_title").text()
            val det = e.getElementsByClass("show_content")
            if (href != null && det != null) {
              if (href.get(0).attributes().get("href").startsWith("/dict/detail/index/")) {
                var num = href.get(0).attributes().get("href").replaceAll("/dict/detail/index/", "")
                var count = det.get(1).text().toInt
                if (count > 2000) dics += ((num, title, count.toString))
              }
            }
          }
        }
        if (dics.length == 0) {
          category = ""
        }
        if (getPages) {
          val pageElms = doc.getElementById("dict_page_list").select("a")
          val pages = ArrayBuffer[Int]()
          var maxpage = 1
          if (pageElms != null) {
            var iter = pageElms.iterator()
            while (iter.hasNext) {
              val e = iter.next()
              if (e.attributes().get("href").startsWith("/dict/cate/index/") && e.text() != "上一页" && e.text() != "下一页" && e.text() != "1") {
                var num = e.text()
                pages += (num.toInt)
              }
            }
            if (pages.length > 0) {
              maxpage = pages.sortWith(_ > _)(0)
              if (maxpage > 20) maxpage = 20
              val allPages = 2 to maxpage
              allPages.foreach { i => {
                val dicsList = mapSogouDicList(dicCategoryId, i, false)("dics").asInstanceOf[ArrayBuffer[(String, String, String)]]
                dicsList.foreach { t => {
                  dics += (t)
                }
                }
              }
              }
            }
          }
        }
        ret = Map(
          "category" -> category,
          "dics" -> dics
        )
      } catch {
        case e:Throwable => println("@@@mapSogouDicList Error: "+e.getClass.getName+": "+e.getMessage)
      }
    }
    ret
  }

  def mapSogouDicInfo(dicid : Int) : Map[String, Any] = {
    val url = "http://pinyin.sogou.com/dict/download_txt.php?id="+dicid
    val urlInfo = mapUrlInfo(url)
    val t1 = System.currentTimeMillis
    var ret = Map[String, Any]()
    ret = Map(
      "charset" -> "",
      "terms" -> Array[String]()
    )
    if (urlInfo("html") != "") {
      try {
        val charset = urlInfo("charset")
        val html = f2J(urlInfo("html"))
        val terms = html.split("\n").map(s => s.trim())
        ret = Map(
          "charset" -> charset,
          "terms" -> terms
        )
      } catch {
        case e:Throwable => println("@@@mapSogouDicInfo Error: "+e.getClass.getName+": "+e.getMessage)
      }
    }
    val t2 = System.currentTimeMillis
    val ts = Math.round((t2 - t1) * 100) / (100 * 1000.00)
    println("###split terms in "+ts+" seconds")
    ret
  }
}
