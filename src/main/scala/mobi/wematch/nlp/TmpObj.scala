package mobi.wematch.nlp

import java.io.{File, FileOutputStream, OutputStreamWriter}

import com.sree.textbytes.readabilityBUNDLE.ContentExtractor
import mobi.wematch.nlp.SparkOps._
import mobi.wematch.nlp.http.HttpOps._
import org.apache.commons.lang.StringEscapeUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.mahout.text.wikipedia.XmlInputFormat
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.feature.{Word2Vec, ChiSqSelector, IDF}
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.json4s.jackson.JsonMethods
import org.jsoup.Jsoup

import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

/**
 * Created by cookeem on 15/8/6.
 */
object TmpObj {
  def main (args: Array[String]) {
    chisqselector()
    //initChisqselector()
  }

  def fileProc(): Unit = {
    val filePath = "/tmp/testdir/"
    val fileName = "a.txt"
    val charset = "UTF-8"
    val dir = new File(filePath)
    if (!dir.exists()) {
      dir.mkdir()
      println("mkdir "+filePath)
    }
    val file = new File(filePath+"/"+fileName)
    if (file.exists()) file.delete()
    file.createNewFile()
    val osw = new OutputStreamWriter(new FileOutputStream(filePath+"/"+fileName, true),charset)
    osw.write("你好，我是曾海剑，你好吗？")
    osw.close()
    val a = Array("a","c","b")
    try {
      val a:String = null
      val c = a.split(" ")
    } catch {
      case e:java.lang.NullPointerException => println("java.lang.NullPointerException")
      case e:Throwable => println("@@@main Error: "+e.getClass.getName+": "+e.getMessage)
    }
  }

  def xmlReader(): Unit = {
    val path = "hdfs://localhost:9000/news_tensite_xml.txt"
    val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY, "<doc>")
    conf.set(XmlInputFormat.END_TAG_KEY, "</doc>")
    val kvs = sc.newAPIHadoopFile(path, classOf[XmlInputFormat],classOf[LongWritable], classOf[Text], conf)
    var i = 1
    kvs.map(t => scala.xml.XML.loadString(t._2.toString)).map(xml => {
      val xmlmap = Map[String, String]()
      xml.child.foreach(node => {
        val key = node.label.toString
        val value = node.text.toString
        if (key != "#PCDATA") xmlmap(key) = value
      })
      xmlmap
    }).take(1).foreach(println(_))
  }

  def htmlContentExtract(url: String): Map[String, String] = {
    val ret = Map[String, String]()
    val urlInfo = mapUrlInfo(url)
    val html =urlInfo("html")
    val ce = new ContentExtractor()
    val article = ce.extractContent(html, "ReadabilitySnack")
    ret("title") = StringEscapeUtils.unescapeHtml(article.getTitle())
    ret("keyword") = StringEscapeUtils.unescapeHtml(article.getMetaKeywords())
    ret("description") = StringEscapeUtils.unescapeHtml(article.getMetaDescription())
    val contentHtml = StringEscapeUtils.unescapeHtml(article.getCleanedArticleText)
    val doc = Jsoup.parse(contentHtml)
    ret("content") = doc.text().toString()
    ret
  }

  def toutiaoJsonExtract(url: String): ArrayBuffer[scala.collection.immutable.Map[String, _]] = {
    val ret = ArrayBuffer[scala.collection.immutable.Map[String, _]]()
    val urlInfo = mapUrlInfo(url)
    val html =urlInfo("html")
    val json = JsonMethods.parse(html)
    (json \ "data").children.foreach(item => {
      ret += item.values.asInstanceOf[scala.collection.immutable.Map[String,_]]
    })
    ret
  }

  def tfidf(): RDD[Vector] = {
    //val str = "Word2Vec computes distributed vector representation of words. The main advantage of the distributed representations is that similar words are close in the vector space, which makes generalization to novel patterns easier and model estimation more robust. Distributed vector representation is showed to be useful in many natural language processing applications such as named entity recognition, disambiguation, parsing, tagging and machine translation."
    //val documents = sc.parallelize(Array(str)).map(l => l.split(" ").toSeq)
    var termArr = Array("0zero","1st","2nd","3th","4th","5th","6th","7th","8th","9th","10th","11th","12th")
    val v = "0 1 1 2 6 7 8 2 5\n0 3 3 4 4 2 1 5\n1 8 6 7 9\n5 1 6 3\n6 8 4 1\n5 5 2 6\n1 4 6 8 1 2".split("\n").map(s => s.split(" ")).map(a => a.map(a => {var ret=0;if (a.trim == "") {ret = 0} else {ret = a.trim.toInt}; ret})).map(a => a.toSeq).toVector
    var seqFeatures = Seq[Vector]()
    v.foreach(seq => {
      val termFrequencies = HashMap[Int, Double]()
      seq.foreach(i =>{
        termFrequencies.put(i,termFrequencies.getOrElse(i,0.0)+1.0)
      })
      val sv = Vectors.sparse(30, termFrequencies.toSeq).asInstanceOf[SparseVector]
      seqFeatures = seqFeatures :+ sv
    })
    val tf = sc.parallelize(seqFeatures)
    tf.cache()
    //训练集文档进行全量计算，得到训练集的idf
    val idf = new IDF().fit(tf)
    //计算所有训练集文档的tfidf
    val tfidf = idf.transform(tf)
    //利用训练集的idf，计算每一篇文档的tfidf
    val v2 = Vectors.sparse(20,Seq((1,1.0),(2,2.0),(5,1.0),(11,2.0)))
    var ab2 = ArrayBuffer[(Int, Double)]()
    val tfidf2 = idf.transform(v2).asInstanceOf[SparseVector]
    val indics = tfidf2.indices
    indics.foreach(i => {
      ab2 += ((i+1,tfidf2(i)))
    })
    ab2.sortWith((t1,t2) => t1._2 > t2._2).foreach(t => {
      print(termArr(t._1-1)+":"+t._2+" ")
    })

    val tab = tfidf.map(vec => {
      var ab = ArrayBuffer[(Int, Double)]()
      var i = 0
      vec.toArray.foreach(vf => {
        i = i + 1
        if (vf != 0.0) {
          println(vf+"->"+i)
          ab += ((i,vf))
        }
      })
      ab.sortWith((t1,t2) => t1._2 > t2._2)
    }).collect()
    tab.foreach(ab => {
      ab.foreach(t =>{
        print(termArr(t._1-1)+":"+t._2+" ")
      })
      println()
    })
    tfidf
  }

  def naivebays(): Unit = {
    var termArr = Array("0zero", "1st", "2nd", "3th", "4th", "5th", "6th", "7th", "8th", "9th", "10th", "11th", "12th")
    val arrVect: Array[Vector] = tfidf().collect()
    var i = 0
    val arrVT = arrVect.map(v => {
      i = i + 1
      (i % 2,v)
    })
    val data = sc.parallelize(arrVT)
    val parsedData = data.map { t =>
      LabeledPoint(t._1.toDouble, t._2)
    }
    val splits = parsedData.randomSplit(Array(0.7, 0.3), seed = 10L)
    val training = splits(0)
    val test = splits(1)
    val model = NaiveBayes.train(training,1.0)
    val predictionAndLabel: RDD[(Double, Double)] = test.map(p => (model.predict(p.features), p.label))
    println("###predictionAndLabel:")
    predictionAndLabel.collect().foreach(t => println(t))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("###accuracy:\n"+accuracy)
    //val sameModel = NaiveBayesModel.load(sc, "myModelPath")
  }

  def chisqselector(): Unit = {
    val data = MLUtils.loadLibSVMFile(sc, "file:///root/mllibdata/mllib/sample_libsvm_data.txt")
    val discretizedData = data.map { lp =>
      LabeledPoint(lp.label, Vectors.dense(lp.features.toArray.map { x => x / 16 } ))
    }
    println("###discretizedData")
    discretizedData.take(2).foreach(lp => println(lp))
    val selector = new ChiSqSelector(20)
    val transformer = selector.fit(discretizedData)
    val sf = transformer.selectedFeatures
    println("###filteredData")
    discretizedData.take(10).map { lp =>
      LabeledPoint(lp.label, transformer.transform(lp.features))
    }.foreach(lp => {
      print(lp.features.size+": ")
      println(lp)
    })
  }

  def w2v(): Unit = {
    val input = sc.textFile("text8").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()

    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("china", 40)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }
  }
}
