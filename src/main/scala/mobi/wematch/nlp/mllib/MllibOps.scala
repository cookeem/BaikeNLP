package mobi.wematch.nlp.mllib

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Vector => BV, argmax => brzArgmax}
import it.nerdammer.spark.hbase._
import mobi.wematch.nlp.SparkOps._
import mobi.wematch.nlp.hbase.HBaseOps._
import mobi.wematch.nlp.mysql.MySqlOps._
import mobi.wematch.nlp.strops.StrOps._
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.feature.{IDF, IDFModel}
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

  def getBaikeKeywords(url: String, topNum: Int): ArrayBuffer[(String, Double)] = {
    var ret = ArrayBuffer[(String, Double)]()
    val rowkey = md5(url)
    val cfName = "info"
    val bkInfo = getTableLocal("urls_baikedone",rowkey,cfName,Map("urlid"->"long"))
    if (bkInfo.size > 0) {
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
      val termsArr = querySql("SELECT termid,term FROM proc_term WHERE termid IN (" + termidsSeq.toArray.mkString(",") + ")")
      val termsMap = Map[Int, String]()
      termsArr.foreach(m => {
        termsMap(m("termid").toInt) = m("term")
      })
      val numArr = querySql("SELECT max(termid) AS termid FROM proc_term")
      val featuresNum = numArr(0)("termid").toInt + 1
      val termFrequencies = HashMap[Int, Double]()
      termidsSeq.foreach(i => {
        termFrequencies.put(i, termFrequencies.getOrElse(i, 0.0) + 1.0)
      })
      val sv: Vector = Vectors.sparse(featuresNum, termFrequencies.toSeq)
      var ab = ArrayBuffer[(String, Double)]()
      var i = 0
      idf.transform(sv).toArray.foreach(v => {
        if (v != 0.0) {
          ab += ((termsMap(i), v))
        }
        i = i + 1
      })
      val sab = ab.sortWith((t1, t2) => t1._2 > t2._2).take(topNum)
      ret = sab
    }
    ret
  }

  def getTagTermids(count: Int = 200): RDD[(String, String, String, String)] = {
    val tagidsQry = querySql("SELECT tagid,tagrootid FROM proc_tags WHERE tagcount >= 500 ORDER BY tagcount DESC LIMIT 5,"+count)
    val tagidsArr = ArrayBuffer[String]()
    val tagidsRootMap = Map[String, String]()
    tagidsQry.foreach(m => {
      tagidsArr += m("tagid")
      if (m("tagrootid") != "0") tagidsRootMap(m("tagid")) = m("tagrootid")
    })
    val cfName = "info"
    val urlsRDD: RDD[(String, String, String, String)] = sc.hbaseTable[(String, String, String, String)]("urls_baikedone").
      select("url", "termids", "tagids").
      inColumnFamily(cfName).
      map(t => (
        (t._1,t._2,t._3),
        t._4.split(" ").map(tagid => {
          var tagrootid = tagid
          if (tagidsRootMap.contains(tagid)) tagrootid = tagidsRootMap(tagid)
          tagrootid
        }).distinct
      )).flatMapValues(t => t).map(t => (t._2,t._1._1,t._1._2,t._1._3)).filter(t => tagidsArr.contains(t._1))
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

  def initNaiveBays(tfTop: Int = 200): Unit = {
    val urlsRDD: RDD[(String, String, String, String)] = getTagTermids()
    val termidsRDD = urlsRDD.map(t => t._4)
    val tf = getTermFreqsVector(termidsRDD)
    val tfidf = idf.transform(tf)

    //tfidf排序抽取topN个单词
    val tfidf2 = tfidf.map(vec => {
      val sv = vec.asInstanceOf[SparseVector]
      val m = Map[Int,Double]()
      sv.indices.foreach(idx => {
        m(idx) = sv(idx)
      })
      val sm: Seq[(Int, Double)] = m.toSeq.sortBy(-_._2).dropRight(sv.indices.length-tfTop)
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

    //NaiveBays多分类输出
    val fnum = 2  //截取的分类数量
    testData.take(50).foreach(p => {
      val brzPi = new BDV[Double](model.pi)
      val brzTheta = new BDM[Double](model.theta.length, model.theta(0).length)
      var i = 0
      while (i < model.theta.length) {
        var j = 0
        while (j < model.theta(i).length) {
          brzTheta(i, j) = model.theta(i)(j)
          j += 1
        }
        i += 1
      }
      val brzVal: breeze.linalg.DenseVector[Double] = brzPi + brzTheta * (BV[Double](p.features.toArray))
      brzVal.toArray.zipWithIndex.sortWith(_._1>_._1).take(fnum).foreach(print(_))
      println()
    })
  }
}
