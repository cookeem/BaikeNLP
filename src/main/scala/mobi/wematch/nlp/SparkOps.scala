package mobi.wematch.nlp

import it.nerdammer.spark.hbase._
import mobi.wematch.nlp.hbase.HBaseOps._
import mobi.wematch.nlp.mllib.MllibOps._
import mobi.wematch.nlp.mysql.MySqlOps._
import mobi.wematch.nlp.strops.StrOps._
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by cookeem on 15/7/27.
 */
object SparkOps {
  def initSpark(): SparkContext = {
    val sparkConf = new SparkConf().setMaster("spark://127.0.0.1:7077").setAppName("nlp")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc
  }
  val sc = initSpark()

  def main (args: Array[String]) {
    val prompt = "###Usage: initall|initbaike|initsogou|getsogouurl|getsogouterm|getbaiketerm|chisqselector|naivebays \n###(multi params support)"
    if (args.length == 0) {
      println("### No params error:\n"+prompt)
    } else {
      try {
        var checked = 0
        if (args.contains("initall")) {
          checked = 1
          println("###Begin to create all tables that needed")
          val cfname = "info"
          try {
            dropTable("urls_incr")
            dropTable("urls_baiketodo")
            dropTable("urls_baikedone")
            dropTable("urls_baikelink")
            dropTable("urls_baiketagid")
            dropTable("urls_dictlist")
            dropTable("urls_dictterm")
            createTable("urls_incr", cfname) //incr：【tableName】【incrvalue】
            createTable("urls_baiketodo", cfname) //等待解析的百科url：【md5(url)】【url】
            createTable("urls_baikedone", cfname) //已经解析的url：【md5(url)】【url redirectUrl charset html content term keyword tags tagids termids】
            createTable("urls_baikelink", cfname) //百科url内部链接表：【md5(fromurl)-md5(tourl)】【fromurl tourl】
            createTable("urls_baiketagid", cfname) //百科url与标签对应表：【md5(tagid)-md5(url)】【url tagid】
            createTable("urls_dictlist", cfname) //等待解析的sogou词库列表url：【md5(url)】【url】
            createTable("urls_dictterm", cfname) //等待下载的sogou词库url：【md5(url)】【url】
            execSql("DROP TABLE IF EXISTS `dics_list`")
            println("###DROP TABLE IF EXISTS `dics_list`")
            execSql("DROP TABLE IF EXISTS `dics_term`")
            println("###DROP TABLE IF EXISTS `dics_term`")
            execSql("DROP TABLE IF EXISTS `proc_term`")
            println("###DROP TABLE IF EXISTS `proc_term`")
            execSql("DROP TABLE IF EXISTS `proc_tags`")
            println("###DROP TABLE IF EXISTS `proc_tags`")
            execSql("CREATE TABLE `dics_list` (   `dicid` int(10) NOT NULL AUTO_INCREMENT COMMENT '搜狗词典ID',   `name` varchar(50) NOT NULL DEFAULT '' COMMENT '词典名称',   `category` varchar(100) NOT NULL DEFAULT '' COMMENT '词典大类',   `count` int(10) NOT NULL DEFAULT '0' COMMENT '词典下载数量',   PRIMARY KEY (`dicid`),   KEY `idx_count` (`count`) USING BTREE ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='搜狗词典表'")
            execSql("CREATE TABLE `dics_term` (   `termid` int(10) NOT NULL AUTO_INCREMENT COMMENT '单词ID',   `term` varchar(40) NOT NULL DEFAULT '' COMMENT '词汇',   `termfreq` int(10) NOT NULL DEFAULT '0' COMMENT '词频',   `dicid` int(10) NOT NULL DEFAULT '0' COMMENT '来源的搜狗词典ID',   PRIMARY KEY (`termid`),   KEY `idx_term` (`term`) USING BTREE ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='搜狗词汇表' ")
            execSql("CREATE TABLE `proc_term` (   `termid` int(10) NOT NULL AUTO_INCREMENT COMMENT '单词ID',   `term` varchar(40) NOT NULL DEFAULT '' COMMENT '词汇',   `termtype` varchar(10) NOT NULL DEFAULT '' COMMENT '单词类型',   `termfreq` int(10) NOT NULL DEFAULT '0' COMMENT '词频',   PRIMARY KEY (`termid`),   KEY `idx_term` (`term`) USING BTREE ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='处理的百科词汇表'")
            execSql("CREATE TABLE `proc_tags` (   `tagid` int(10) NOT NULL AUTO_INCREMENT COMMENT '标签ID',   `tag` varchar(40) NOT NULL DEFAULT '' COMMENT '标签',   `tagcount` int(10) NOT NULL DEFAULT '0' COMMENT '标签出现次数',   PRIMARY KEY (`tagid`),   KEY `idx_tag` (`tag`) USING BTREE ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='处理的百科标签表'")
          } catch {
            case e:Throwable => println("@@@initTables Error: "+e.getClass.getName+": "+e.getMessage)
          }
          println("###Finish create all tables that needed")
        }
        if (args.contains("initbaike")) {
          checked = 1
          var t1 = System.currentTimeMillis
          println("###Begin to init all baike tables that needed")
          truncateTable("urls_baiketodo")
          truncateTable("urls_baikedone")
          deleteRowkey("urls_incr", "urls_baiketodo")
          deleteRowkey("urls_incr", "urls_baikedone")
          execSql("truncate table proc_tags")
          val brdd = sc.parallelize(1 to 17200531).map(i => "http://baike.baidu.com/view/" + i.toString + ".htm").map(url => (md5(url), url))
          brdd.toHBaseTable("urls_baiketodo").toColumns("url").inColumnFamily("info").save()
          val ts = Math.round((System.currentTimeMillis - t1) * 100) / (100 * 1000.00)
          println("###Finish init all baike tables that needed in " + ts + " seconds")
        }
        if (args.contains("initsogou")) {
          checked = 1
          var t1 = System.currentTimeMillis
          println("###Begin to init all sogou tables that needed")
          truncateTable("urls_dictlist")
          truncateTable("urls_dictterm")
          deleteRowkey("urls_incr", "urls_dictlist")
          deleteRowkey("urls_incr", "urls_dictterm")
          execSql("truncate table dics_list")
          execSql("truncate table dics_term")
          execSql("truncate table proc_term")
          execSql("truncate table proc_tags")
          val srdd = sc.parallelize(1 to 623).map(i => "http://pinyin.sogou.com/dict/cate/index/" + i.toString + "/download/1").map(url => (md5(url), url))
          srdd.toHBaseTable("urls_dictlist").toColumns("url").inColumnFamily("info").save()
          val ts = Math.round((System.currentTimeMillis - t1) * 100) / (100 * 1000.00)
          println("###Finish init all sogou tables that needed in " + ts + " seconds")
        }
        if (args.contains("getsogouurl")) {
          checked = 1
          val cfName = "info"
          val hbaseRDD = sc.hbaseTable[(String, String)]("urls_dictlist").select("url").inColumnFamily(cfName).map(t => (t._2))
          hbaseRDD.foreach(url => {
            parseSogouDicUrlToHbase(url)
          })
        }
        if (args.contains("getsogouterm")) {
          checked = 1
          val cfName = "info"
          val hbaseRDD = sc.hbaseTable[(String, String)]("urls_dictterm").select("url").inColumnFamily(cfName).map(t => (t._2))
          hbaseRDD.foreach(url => {
            parseSogouDicTermToMysql(url)
          })
          //去除无用的词典
          println("###Begin to remove no usage dicts###")
          execSql("update dics_list set count = 0 where category like '%游戏%'")
          execSql("update dics_list set count = 0 where category like '%娱乐%'")
          execSql("update dics_list set count = 0 where category like '%武侠%'")
          execSql("update dics_list set count = 0 where category like '%个人%'")
          execSql("update dics_list set count = 0 where category like '%收藏%'")
          execSql("update dics_list set count = 0 where category like '%机器猫%'")
          execSql("update dics_list set count = 0 where category like '%搜狗细胞词库%'")
          execSql("update dics_list set count = 0 where category like '%哲学%'")
          execSql("update dics_list set count = 0 where category like '%玄幻%'")
          execSql("update dics_list set count = 0 where category like '%其它%'")
          execSql("update dics_list set count = 0 where category like '%文学%'")
          execSql("update dics_list set count = 0 where category like '%方言%'")
          execSql("delete from dics_term where term like '的%'")
        }
        if (args.contains("getbaiketerm")) {
          checked = 1
          val cfName = "info"
          val hbaseRDD = sc.hbaseTable[(String, String)]("urls_baiketodo").select("url").inColumnFamily(cfName).map(t => (t._2))
          //val hbaseRDD = sc.parallelize(1 to 2).map(i => "http://baike.baidu.com/view/"+i+".htm")
          hbaseRDD.foreachPartition(epr => {
            loadAnsjLib()
            epr.foreach(url => {
              parseBaikeTermToHbase(url)
            })
          })
        }
        if (args.contains("chisqselector")) {
          checked = 1
          initChisqselector()
        }
        if (args.contains("naivebays")) {
          checked = 1
          initNaiveBays()
        }
        if (checked == 0) {
          println("### Params not correct error:\n" + prompt)
        }
      } catch {
        case e:Throwable => println("@@@SparkOps.main Error: "+e.getClass.getName+": "+e.getMessage)
      }
    }
  }
}

