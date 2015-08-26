package mobi.wematch.nlp.mllib

import it.nerdammer.spark.hbase._
import mobi.wematch.nlp.SparkOps._
import mobi.wematch.nlp.hbase.HBaseOps._
import mobi.wematch.nlp.mysql.MySqlOps._
import mobi.wematch.nlp.strops.StrOps._
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.feature.{ChiSqSelector, IDF, IDFModel}
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

/**
 * Created by cookeem on 15/8/13.
 */
object MllibOps {
  def initIdf(): IDFModel = {
    val cfName = "info"
    val inputRDD = sc.hbaseTable[(String, Option[String])]("urls_baikedone").select("termids").inColumnFamily("info").filter(t => t._2.nonEmpty).map(t => (t._1, t._2.get.toString)).filter(t => t._2 != "")
    val numArr = querySql("select max(termid) as termid from proc_term")
    val featuresNum = numArr(0)("termid").toInt+1
    val seqRDD = inputRDD.map(t => {
      val urlRowkey = t._1
      val termids = t._2
      val termidsSeq: Seq[Int] = termids.split(" ").map(str => {
        var ret = 0
        if (str.trim == "") {
          ret = 0
        } else {
          ret = str.trim.toInt
        }
        ret
      }).toSeq
      termidsSeq
    })
    val tf: RDD[Vector] = seqRDD.map(seq => {
      val termFrequencies = HashMap[Int, Double]()
      seq.foreach(i =>{
        termFrequencies.put(i,termFrequencies.getOrElse(i,0.0)+1.0)
      })
      val sv: Vector = Vectors.sparse(featuresNum, termFrequencies.toSeq)
      sv
    })
    //tf.cache()
    //训练集文档进行全量计算，得到训练集的idf
    val idf: IDFModel = new IDF().fit(tf)
    idf
  }

  val idf = initIdf()

  def getUrlKeywords(url: String, topNum: Int): ArrayBuffer[(String, Double)] = {
    val rowkey = md5(url)
    val cfName = "info"
    val bkInfo = getTableLocal("urls_baikedone",rowkey,cfName,Map("urlid"->"long"))
    val termids = bkInfo("termids").asInstanceOf[String]
    val termidsSeq: Seq[Int] = termids.split(" ").map(str => {
      var ret = 0
      if (str.trim == "") {
        ret = 0
      } else {
        ret = str.trim.toInt
      }
      ret
    }).toSeq
    val termsArr = querySql("SELECT termid,term FROM proc_term WHERE termid IN ("+termidsSeq.toArray.mkString(",")+")")
    val termsMap = Map[Int, String]()
    termsArr.foreach(m => {
      termsMap(m("termid").toInt) = m("term")
    })
    val numArr = querySql("SELECT max(termid) AS termid FROM proc_term")
    val featuresNum = numArr(0)("termid").toInt
    val termFrequencies = HashMap[Int, Double]()
    termidsSeq.foreach(i =>{
      termFrequencies.put(i,termFrequencies.getOrElse(i,0.0)+1.0)
    })
    val sv: Vector = Vectors.sparse(featuresNum, termFrequencies.toSeq)
    var ab = ArrayBuffer[(String, Double)]()
    var i = 0
    idf.transform(sv).toArray.foreach(v => {
      if (v != 0.0) {
        ab += ((termsMap(i),v))
      }
      i = i + 1
    })
    val sab = ab.sortWith((t1,t2) => t1._2 > t2._2).take(topNum)
    sab
  }

  def getTagTermids(count: Int = 50): RDD[(String, String, String, String)] = {
    val tagidsQry = querySql("SELECT tagid FROM proc_tags WHERE tagcount >= 500 ORDER BY tagcount DESC LIMIT 5,"+count)
    val tagidsArr = ArrayBuffer[String]()
    tagidsQry.foreach(m => {
      tagidsArr += m("tagid")
    })
    val cfName = "info"
    val urlsRDD: RDD[(String, String, String, String)] = sc.hbaseTable[(String, String, String, String)]("urls_baikedone").select("url", "termids", "tagids").inColumnFamily(cfName).map(t => ((t._1,t._2,t._3),t._4.split(" "))).flatMapValues(t => t).map(t => (t._2,t._1._1,t._1._2,t._1._3)).filter(t => tagidsArr.contains(t._1))
    urlsRDD
  }

  def getTermFreqsVector(rdd: RDD[String], getCount: Boolean = true): RDD[Vector] = {
    val numArr = querySql("SELECT max(termid) AS termid FROM proc_term")
    val featuresNum = numArr(0)("termid").toInt
    val termFrequencies = HashMap[Int, Double]()
    val termFreq: RDD[Vector] = rdd.map(termids => {
      val seq = termids.split(" ").map(str => {
        var ret = 0
        if (str.trim == "") {
          ret = 0
        } else {
          ret = str.trim.toInt
        }
        ret
      }).toSeq
      val termFrequencies = HashMap[Int, Double]()
      seq.foreach(i => {
        if (getCount) {
          termFrequencies.put(i, termFrequencies.getOrElse(i, 0.0) + 1.0)
        } else {
          termFrequencies.put(i, 1.0)
        }
      })
      val sv: Vector = Vectors.sparse(featuresNum, termFrequencies.toSeq)
      sv
    })
    termFreq
  }

  def initNaiveBays(): Unit = {
    val urlsRDD: RDD[(String, String, String, String)] = getTagTermids(5)
    val termidsRDD = urlsRDD.map(t => t._4)
    val tf = getTermFreqsVector(termidsRDD)
    val tfidf = idf.transform(tf)
    //tfidf排序抽取top50
    val tfidf2 = tfidf.map(vec => {
      val sv = vec.asInstanceOf[SparseVector]
      val m = Map[Int,Double]()
      sv.indices.foreach(idx => {
        m(idx) = sv(idx)
      })
      val sm: Seq[(Int, Double)] = m.toSeq.sortBy(-_._2).dropRight(sv.indices.length-50)
      val keys = sm.map(t => t._1).toArray
      val vals = sm.map(t => t._2).toArray
      val sv2 = Vectors.sparse(sv.size,keys,vals)
      sv2
    })
    val parsedData: RDD[LabeledPoint] = urlsRDD.map(t => t._1.toInt).zip(tfidf2).map(t => LabeledPoint(t._1.toDouble, t._2))
    val splits = parsedData.randomSplit(Array(0.7, 0.3), seed = 11L)
    val trainData = splits(0)
    val testData = splits(1)
    val model = NaiveBayes.train(trainData,1.0)
    val predictionAndLabel: RDD[(Double, Double)] = testData.map(p => (model.predict(p.features), p.label))
    println("###predictionAndLabel:")
    predictionAndLabel.take(5).foreach(t => println(t))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testData.count()
    println("###accuracy:\n"+accuracy)
  }

  def initChisqselector(): Unit = {
    val urlsRDD: RDD[(String, String, String, String)] = getTagTermids(5)
    val termidsRDD = urlsRDD.map(t => t._4)
    val tf: RDD[Vector] = getTermFreqsVector(termidsRDD,false)
    //val tfidf: RDD[Vector] = idf.transform(tf)
    //val parsedData: RDD[LabeledPoint] = urlsRDD.map(t => t._1.toInt).zip(tfidf).map(t => LabeledPoint(t._1.toDouble, t._2))
    val parsedData: RDD[LabeledPoint] = urlsRDD.map(t => t._1.toInt).zip(tf).map(t => LabeledPoint(t._1.toDouble, t._2))
    println("###parsedData")
    parsedData.take(2).foreach(lp => println(lp))
    val selector = new ChiSqSelector(50)
    val transformer = selector.fit(parsedData)
    val filteredData = parsedData.map { lp =>
      LabeledPoint(lp.label, transformer.transform(lp.features))
    }
    println("###filteredData")
    filteredData.take(2).foreach(lp => println(lp))
    val seq = Seq(1 to 1000)
    seq.__leftOfArrow
  }
}
