package com.bfd.api

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import net.sf.json.JSONObject
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


/**
  * @author ${user.name}
  */
object AccessLogJob {

  val infologger = LoggerFactory.getLogger("info")
  val errorlogger = LoggerFactory.getLogger("error")
  val warnlogger = LoggerFactory.getLogger("warn")

  def main(args: Array[String]) {
    //读取配置
    if (args.length != 1) {
      println("Usage spark-submit   --class com.bfd.api.AccessLogJob  --master yarn  --name accessLogStreaming  --conf spark.streaming.backpressure.enabled=true --conf spark.streaming.receiver.maxRate=200000  --jars bfdjodis-0.1.2-jar-with-dependencies.jar  --num-executors 10  --driver-memory 10g  --executor-memory 9g  --executor-cores 20 online.ids.graph-0.0.1-SNAPSHOT-jar-with-dependencies.jar online.properties")
    }

    val prop = new Properties()
    val in = new FileInputStream(args(0))
    prop.load(in)
    in.close()

    val zks = prop.getProperty("zks")
    val appName = prop.getProperty("log.app.name")
    val master = prop.getProperty("spark.master")
    val group = prop.getProperty("kafka.group.id")
    val topics = prop.getProperty("kafka.topics")
    val numThreads = prop.getProperty("kafka.numThreads")
    val accessLogTabel = prop.getProperty("hbase.access.log.table")
    val zkquorum = prop.getProperty("hbase.zookeeper.quorum")
    val isNumber = java.lang.Integer.valueOf(prop.getProperty("isNumber"))
    val isFrequency = java.lang.Integer.valueOf(prop.getProperty("isFrequency"))
    val isFlow = java.lang.Integer.valueOf(prop.getProperty("isFlow"))
    val freeAllowance = java.lang.Long.valueOf(prop.getProperty("freeAllowance"))
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    //一秒一个批次
    val ssc = new StreamingContext(conf, Seconds(60))

    try {
      //读取kafka
      val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
      ssc.checkpoint("checkpoint")
      val kafkaStream = KafkaUtils.createStream(ssc, zks, group, topicMap)
      val events = kafkaStream.flatMap(line => {
        val data = JSONObject.fromObject(line._2)
        Some(data)
      })

      events.foreachRDD((rdd: RDD[JSONObject], time: Time) => {
        //写hbase
        rdd.foreachPartition(partitionOfRecords => {
          if (partitionOfRecords.isEmpty) {
          } else {
            val hbaseConf = HBaseConfiguration.create()
            val accessLogTabel = "api_log:zuul_access_log"
            hbaseConf.set("hbase.zookeeper.quorum", zkquorum)
            hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
            hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
            hbaseConf.setInt("hbase.rpc.timeout", 20000)
            hbaseConf.setInt("hbase.client.operation.timeout", 30000)
            hbaseConf.setInt("hbase.client.scanner.timeout.period", 200000)
            val ugi = UserGroupInformation.createRemoteUser("hdfs")
            val user = User.create(ugi)

            val hbaseConn = ConnectionFactory.createConnection(hbaseConf, user)
            val tableName = TableName.valueOf(accessLogTabel)
            val table = hbaseConn.getTable(tableName)
            try {
              val list = new util.ArrayList[Put]()
              partitionOfRecords.foreach(pair => {
                //插入hbase
                val rowkeystr = pair.getString("userId") + "_" + pair.getString("apiId") + "_" + pair.getString("appId") + "_" + pair.getString("addTime")
                val rowkey = DigestUtils.md5Hex(rowkeystr).substring(0, 2) + "_" + rowkeystr
                println("rowkey：" + rowkey)
                val put = new Put(Bytes.toBytes(rowkey))
                if (StringUtils.isNotBlank(pair.getString("apiId"))) {
                  println("apiId：" + pair.getString("apiId"))
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("api_id"), Bytes.toBytes(pair.getString("apiId")))
                }
                if (StringUtils.isNotBlank(pair.getString("appId"))) {
                  println("apiId：" + pair.getString("apiId"))
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("app_id"), Bytes.toBytes(pair.getString("appId")))
                }
                if (StringUtils.isNotBlank(pair.getString("userId"))) {
                  println("userId：" + pair.getString("userId"))
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("user_id"), Bytes.toBytes(pair.getString("userId")))
                }
                if (StringUtils.isNotBlank(pair.getString("userLogo"))) {
                  println("userLogo：" + pair.getString("userLogo"))
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("user_logo"), Bytes.toBytes(pair.getString("userLogo")))
                }
                if (StringUtils.isNotBlank(pair.getString("userProject"))) {
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("user_project"), Bytes.toBytes(pair.getString("userProject")))
                }
                if (StringUtils.isNotBlank(pair.getString("appCode"))) {
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("app_code"), Bytes.toBytes(pair.getString("appCode")))
                }
                if (StringUtils.isNotBlank(pair.getString("apiType"))) {
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("api_type"), Bytes.toBytes(pair.getString("apiType")))
                }

                if (StringUtils.isNotBlank(pair.getString("url"))) {
                  println("url：" + pair.getString("url"))
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("url"), Bytes.toBytes(pair.getString("url")))
                }

                if (StringUtils.isNotBlank(pair.getString("inParam"))) {
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("in_param"), Bytes.toBytes(pair.getString("inParam")))
                }

                if (StringUtils.isNotBlank(pair.getString("useTime"))) {
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("use_time"), Bytes.toBytes(pair.getString("useTime")))
                }

                if (StringUtils.isNotBlank(pair.getString("ip"))) {
                  println("ip：" + pair.getString("ip"))
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("ip"), Bytes.toBytes(pair.getString("ip")))
                }

                if (StringUtils.isNotBlank(pair.getString("status"))) {
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("status"), Bytes.toBytes(pair.getString("status")))
                }

                if (StringUtils.isNotBlank(pair.getString("message"))) {
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("message"), Bytes.toBytes(pair.getString("message")))
                }

                if (StringUtils.isNotBlank(pair.getString("accessKey"))) {
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("access_key"), Bytes.toBytes(pair.getString("accessKey")))
                }

                if (StringUtils.isNotBlank(pair.getString("addTime"))) {
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("add_time"), Bytes.toBytes(pair.getString("addTime")))
                }
                if (StringUtils.isNotBlank(pair.getString("chargeType"))) {
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("chargeType"), Bytes.toBytes(pair.getString("chargeType")))
                }
                if (StringUtils.isNotBlank(pair.getString("accessFlow"))) {
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("accessFlow"), Bytes.toBytes(pair.getString("accessFlow")))
                }
                if (StringUtils.isNotBlank(pair.getString("dealCount"))) {
                  put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dealCount"), Bytes.toBytes(pair.getString("dealCount")))
                }
                list.add(put)
              })
              println("list：" + list.toString)

              table.put(list)
            } catch {
              case e: Exception => print("exception caught: " + e);
            } finally {
              if (table != null) table.close()
            }
          }
        })

        //计算
        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._
        val logDataFrame = rdd.map(
          w => {
            AccessLog(w.getString("userId"), w.getString("userLogo"), w.getString("userProject")
              , java.lang.Long.valueOf(w.getString("appId")), w.getString("appCode")
              , java.lang.Long.valueOf(w.getString("apiId")), w.getString("url"), w.getInt("apiType"), w.getString("inParam")
              , w.getString("useTime"), w.getString("ip"), w.getString("addTime"), w.getInt("status")
              , w.getString("message"), w.getString("accessKey"), w.getInt("chargeType")
              , java.lang.Long.valueOf(w.getString("accessFlow")), java.lang.Long.valueOf(w.getString("dealCount"))
            );
          }
        ).toDF()
        logDataFrame.registerTempTable("accessLog")

        logDataFrame.show(10)
        println(logDataFrame.count())

        val userCountsDataFrame = sqlContext.sql("select userId,apiId,appId,status,inParam," +
          "count(1) as count,first(userLogo) as userLogo,sum(accessFlow) as sumFlow," +
          "sum(dealCount) as sumCount,first(userProject) as userProject," +
          "first(apiType) as apiType,first(chargeType) as chargeType," +
          "first(addTime) as addTime from accessLog " +
          "group by userId,apiId,appId,status,inParam")
        userCountsDataFrame.collect()
        userCountsDataFrame.show()

        val userCountsRowRDD = userCountsDataFrame.rdd

        userCountsRowRDD.foreachPartition { partitionOfRecords => {
          if (partitionOfRecords.isEmpty) {
            println("this is RDD is not null but partition is null")
          } else {
            val connection = ConnectionPool.getConnection.getOrElse(null)
            try
              partitionOfRecords.foreach(record => {
                //1，mysql 用户访问统计
                var userId = record.getAs("userId").toString
                var userLogo = record.getAs("userLogo").toString
                var userProject = record.getAs("userProject").toString
                var apiId = java.lang.Long.valueOf(record.getAs("apiId").toString)
                var appId = java.lang.Long.valueOf(record.getAs("appId").toString)
                var count = java.lang.Long.valueOf(record.getAs("count").toString)
                val status = record.getAs("status").toString
                //日期
                val addtime = java.lang.Long.valueOf(record.getAs("addTime").toString)
                val format2 = new SimpleDateFormat("yyyy-MM-dd")
                val format3 = new SimpleDateFormat("yyyy-MM")
                val accessday = format2.format(addtime)
                val accessmonth = format3.format(addtime)
                var apiType = record.getAs("apiType").toString
                var chargeType = java.lang.Integer.valueOf(record.getAs("chargeType").toString)
                var sumFlow = java.lang.Long.valueOf(record.getAs("sumFlow").toString)
                var sumCount = java.lang.Long.valueOf(record.getAs("sumCount").toString)
                println("----------------------------------sumCount" + sumCount)
                var inParam = record.getAs("inParam").toString
                val user_access_stat_sql = "INSERT INTO user_access_statistics(user_id,user_logo,user_project,api_id,app_id," +
                  "access_count,success_count,access_day,update_time,access_flow,deal_count) " +
                  "VALUES (?,?,?,?,?,?,?,?,NOW(),?,?) ON DUPLICATE KEY " +
                  "UPDATE access_count = access_count + ?,success_count = success_count + ?, update_time = NOW(),access_flow=access_flow+?,deal_count=deal_count+?";
                val user_access_stat = connection.prepareStatement(user_access_stat_sql)
                user_access_stat.setString(1, userId)
                user_access_stat.setString(2, userLogo)
                user_access_stat.setString(3, userProject)
                user_access_stat.setLong(4, apiId)
                user_access_stat.setLong(5, appId)
                user_access_stat.setLong(6, count)
                user_access_stat.setLong(11, count)
                if (status.equals("1")) {
                  user_access_stat.setLong(7, count)
                  user_access_stat.setLong(12, count)
                  user_access_stat.setLong(9, sumFlow)
                  user_access_stat.setLong(13, sumFlow)
                  user_access_stat.setLong(10, sumCount)
                  user_access_stat.setLong(14, sumCount)
                } else {
                  user_access_stat.setLong(7, 0)
                  user_access_stat.setLong(12, 0)
                  user_access_stat.setLong(9, 0)
                  user_access_stat.setLong(13, 0)
                  user_access_stat.setLong(10, 0)
                  user_access_stat.setLong(14, 0)
                }
                //处理日期
                user_access_stat.setString(8, accessday)

                val sqlnum_user_access_stat = user_access_stat.executeUpdate()
                if (sqlnum_user_access_stat == 0) {
                  errorlogger.error("user access stat insert mysql error")
                }

                //2，mysql 项目访问统计
                val project_access_stat_sql = "INSERT INTO project_access_statistics(logo,project,api_id," +
                  "access_count,success_count,access_day,update_time,access_flow,deal_count) " +
                  "VALUES (?,?,?,?,?,?,NOW(),?,?) ON DUPLICATE KEY " +
                  "UPDATE access_count = access_count + ?,success_count = success_count + ?, update_time = NOW(),access_flow=access_flow+?,deal_count=deal_count+?";
                val project_access_stat = connection.prepareStatement(project_access_stat_sql)
                project_access_stat.setString(1, userLogo)
                project_access_stat.setString(2, userProject)
                project_access_stat.setLong(3, apiId)
                project_access_stat.setLong(4, count)
                project_access_stat.setLong(9, count)
                if (status.equals("1")) {
                  project_access_stat.setLong(5, count)
                  project_access_stat.setLong(10, count)
                  project_access_stat.setLong(7, sumFlow)
                  project_access_stat.setLong(11, sumFlow)
                  project_access_stat.setLong(8, sumCount)
                  project_access_stat.setLong(12, sumCount)
                } else {
                  project_access_stat.setLong(5, 0)
                  project_access_stat.setLong(8, 0)
                  project_access_stat.setLong(7, 0)
                  project_access_stat.setLong(11, 0)
                  project_access_stat.setLong(8, 0)
                  project_access_stat.setLong(12, 0)
                }
                //处理日期
                project_access_stat.setString(6, accessday)

                val sqlnum_project_access_stat = project_access_stat.executeUpdate()
                if (sqlnum_project_access_stat == 0) {
                  errorlogger.error("project access stat insert mysql error")
                }
                var isFree = 0 //是否免费0不免费,1免费
                try {
                  val dataJson = JSONObject.fromObject(inParam)
                  var inUserProject = dataJson.get("accessProject").toString
                  if (inUserProject.contains(userProject)) {
                    isFree = 1
                  }
                } catch {
                  case ex: NullPointerException => {
                    println("accessProject is not fand")
                  }
                }
                if (apiType.equals("1") && isFree == 0 && (!(userId.toString.equals("-1") || userId.toString.equals("-") || userId.toString.equals("1")))) {
                  //收费
                  var sum_access_count_all = 0L
                  //总的次数
                  var sum_access_flow_all = 0L
                  //总的流量
                  var sum_deal_count_all = 0L
                  //总的数量
                  var count_access_all_sql = "select sum(access_count) as sum_access_count,sum(access_flow) as sum_access_flow,sum(deal_count) as sum_deal_count from user_charging_day " +
                    "where user_id=? and user_logo=? and user_project=? and api_id=? and charge_type=?"
                  val count_access_all_sql_stat = connection.prepareStatement(count_access_all_sql)
                  count_access_all_sql_stat.setString(1, userId)
                  count_access_all_sql_stat.setString(2, userLogo)
                  count_access_all_sql_stat.setString(3, userProject)
                  count_access_all_sql_stat.setLong(4, apiId)
                  count_access_all_sql_stat.setInt(5, chargeType)
                  var count_access_all_resultSet = count_access_all_sql_stat.executeQuery()
                  while (count_access_all_resultSet.next()) {
                    sum_access_count_all = count_access_all_resultSet.getLong("sum_access_count")
                    sum_access_flow_all = count_access_all_resultSet.getLong("sum_access_flow")
                    sum_deal_count_all = count_access_all_resultSet.getLong("sum_deal_count")
                  }
                  var sum_access_count = 0L
                  //本月总的次数
                  var sum_access_flow = 0L
                  //本月总的流量
                  var sum_deal_count = 0L
                  //本月总的数量
                  var count_access_sql = "select sum(access_count) as sum_access_count,sum(access_flow) as sum_access_flow,sum(deal_count) as sum_deal_count from user_charging_day " +
                    "where user_id=? and user_logo=? and user_project=? and api_id=? and charge_type=? and substring(access_day,1,7)=?"
                  val count_access_sql_stat = connection.prepareStatement(count_access_sql)
                  count_access_sql_stat.setString(1, userId)
                  count_access_sql_stat.setString(2, userLogo)
                  count_access_sql_stat.setString(3, userProject)
                  count_access_sql_stat.setLong(4, apiId)
                  count_access_sql_stat.setInt(5, chargeType)
                  count_access_sql_stat.setString(6, accessmonth)
                  var count_access_resultSet = count_access_sql_stat.executeQuery()
                  while (count_access_resultSet.next()) {
                    sum_access_count = count_access_resultSet.getLong("sum_access_count")
                    sum_access_flow = count_access_resultSet.getLong("sum_access_flow")
                    sum_deal_count = count_access_resultSet.getLong("sum_deal_count")
                  }
                  var chargCount = 0L
                  //本次计费数量
                  var sumChargCount = 0L
                  //本月总的计费数量
                  var chargedDealCount = sum_deal_count //本月已计费数量
                  if (sumCount + sum_deal_count_all <= freeAllowance) {
                    chargCount = 0L
                    isFree = 1
                    chargedDealCount = 0
                  } else if (sum_deal_count_all > freeAllowance) {
                    chargCount = sumCount
                    var addCharg = if (sum_deal_count_all - sum_deal_count >= freeAllowance) 0 else freeAllowance - (sum_deal_count_all - sum_deal_count)
                    sumChargCount = sumCount + sum_deal_count - addCharg
                    chargedDealCount = sum_deal_count - addCharg
                  } else {
                    var addCharg = freeAllowance - sum_deal_count_all
                    chargCount = sumCount - addCharg
                    sumChargCount = chargCount
                    chargedDealCount = 0
                  }
                  var charge_id = 0L
                  //收费类型编码
                  var unit_price = 0.0
                  //单价
                  var is_discount = 1
                  //是否优惠
                  var discount = 1.0
                  //优惠折扣
                  var lower_limit = 0l
                  //计费下限
                  var upper_limit = 0l
                  //计费上限
                  var sumMoney = 0.0
                  var countMore = 0L
                  var chargedCount = sum_access_count
                  //已计费次数
                  var chargedFlow = sum_access_flow
                  //已计费流量
                  var sumChargCounts = count + sum_access_count
                  //总的计费次数
                  var sumChargFlow = count + sum_access_flow //总的计费流量
                  if (isFree == 0) {
                    var charge_id_sql = "select id,unit_price,is_discount,discount,upper_limit,lower_limit from charge_rule where spend_type=? and lower_limit<=? and upper_limit>=?+1 and status=1"
                    val charge_id_sql_stat = connection.prepareStatement(charge_id_sql)
                    charge_id_sql_stat.setInt(1, chargeType)
                    if (status.equals("1")) {
                      if (chargeType == isFrequency) {
                        charge_id_sql_stat.setLong(2, sumChargCounts)
                        charge_id_sql_stat.setLong(3, sum_access_count)
                      } else if (chargeType == isNumber) {
                        charge_id_sql_stat.setLong(2, sumChargCount)
                        charge_id_sql_stat.setLong(3, chargedDealCount)
                      } else {
                        charge_id_sql_stat.setLong(2, sumChargFlow)
                        charge_id_sql_stat.setLong(3, sum_access_flow)
                      }
                    }
                    var charge_id_resultSet = charge_id_sql_stat.executeQuery()
                    countMore = count
                    while (charge_id_resultSet.next()) {
                      charge_id = charge_id_resultSet.getLong("id")
                      unit_price = charge_id_resultSet.getDouble("unit_price")
                      is_discount = charge_id_resultSet.getInt("is_discount")
                      discount = (charge_id_resultSet.getLong("discount").toDouble) / 100
                      lower_limit = charge_id_resultSet.getLong("lower_limit")
                      upper_limit = charge_id_resultSet.getLong("upper_limit")
                      if (status.equals("1")) {
                        if (chargeType == isFrequency) {
                          if (sumChargCounts >= upper_limit && chargedCount <= lower_limit) {
                            sumMoney = sumMoney + (upper_limit - lower_limit + 1) * unit_price * discount
                            chargedCount = upper_limit
                          } else if (sumChargCounts >= upper_limit && chargedCount > lower_limit) {
                            sumMoney = sumMoney + (upper_limit - chargedCount) * unit_price * discount
                            chargedCount = upper_limit
                          } else {
                            sumMoney = sumMoney + (sumChargCounts - chargedCount) * unit_price * discount
                          }
                        } else if (chargeType == isNumber) {
                          if (sumChargCount >= upper_limit && chargedDealCount < lower_limit) {
                            sumMoney = sumMoney + (upper_limit - lower_limit + 1) * unit_price * discount
                            chargedDealCount = upper_limit
                          } else if (sumChargCount >= upper_limit && chargedDealCount >= lower_limit) {
                            sumMoney = sumMoney + (upper_limit - chargedDealCount) * unit_price * discount
                            chargedDealCount = upper_limit
                          }
                          else {
                            if (chargedDealCount < lower_limit) {
                              chargCount = sumChargCount - chargedDealCount
                            }
                            sumMoney = sumMoney + chargCount * unit_price * discount
                          }
                        } else {
                          if (sumChargFlow >= upper_limit && chargedFlow <= lower_limit) {
                            sumMoney = sumMoney + (upper_limit - lower_limit + 1) * unit_price * discount
                            chargedFlow = upper_limit
                          } else if (sumChargFlow >= upper_limit && chargedFlow > lower_limit) {
                            sumMoney = sumMoney + (upper_limit - chargedCount) * unit_price * discount
                            chargedFlow = upper_limit
                          } else {
                            sumMoney = sumMoney + (sumChargFlow - chargedFlow) * unit_price * discount
                          }
                        }
                      }
                    }
                    sumMoney = if (Math.round(sumMoney) % 2 == 0) Math.round(sumMoney) else Math.round(sumMoney) + 1
                    if (charge_id == 0L) {
                      errorlogger.error("charge  id  can not find")
                    }
                  }
                  //3，mysql 用户计费统计天、月
                  //用户天
                  val user_charging_day_stat_sql = "INSERT INTO user_charging_day(user_id,user_logo,user_project,api_id," +
                    "access_count,access_day,charge_type,charge_id,access_money,update_time,access_flow,deal_count) " +
                    "VALUES (?,?,?,?,?,?,?,?,?,NOW(),?,?) ON DUPLICATE KEY " +
                    "UPDATE access_count = access_count + ?,access_money=access_money+?,update_time = NOW()," +
                    "access_flow=access_flow+?,deal_count=deal_count+?,charge_id=?"
                  val user_charging_day_stat = connection.prepareStatement(user_charging_day_stat_sql)
                  user_charging_day_stat.setString(1, userId)
                  user_charging_day_stat.setString(2, userLogo)
                  user_charging_day_stat.setString(3, userProject)
                  user_charging_day_stat.setLong(4, apiId)
                  user_charging_day_stat.setInt(7, chargeType)
                  if (status.equals("1")) {
                    user_charging_day_stat.setLong(5, countMore)
                    user_charging_day_stat.setLong(12, countMore)
                    user_charging_day_stat.setDouble(9, sumMoney)
                    user_charging_day_stat.setDouble(13, sumMoney)
                    user_charging_day_stat.setLong(10, sumFlow)
                    user_charging_day_stat.setLong(14, sumFlow)
                    user_charging_day_stat.setLong(11, sumCount)
                    user_charging_day_stat.setLong(15, sumCount)
                    user_charging_day_stat.setLong(8, charge_id)
                    user_charging_day_stat.setLong(16, charge_id)
                  }
                  //处理日期
                  user_charging_day_stat.setString(6, accessday)
                  val sqlnum_user_charging_day_stat = user_charging_day_stat.executeUpdate()
                  if (sqlnum_user_charging_day_stat == 0) {
                    errorlogger.error("user charging day stat insert mysql error")
                  }
                  //用户月
                  val user_charging_month_stat_sql = "INSERT INTO user_charging_month(user_id,user_logo,user_project,api_id," +
                    "access_count,access_month,charge_type,charge_id,access_money,update_time,access_flow,deal_count) " +
                    "VALUES (?,?,?,?,?,?,?,?,?,NOW(),?,?) ON DUPLICATE KEY " +
                    "UPDATE access_count = access_count + ?,access_money=access_money+?,update_time = NOW()," +
                    "access_flow=access_flow+?,deal_count=deal_count+?,charge_id=?"
                  val user_charging_month_stat = connection.prepareStatement(user_charging_month_stat_sql)
                  user_charging_month_stat.setString(1, userId)
                  user_charging_month_stat.setString(2, userLogo)
                  user_charging_month_stat.setString(3, userProject)
                  user_charging_month_stat.setLong(4, apiId)
                  user_charging_month_stat.setInt(7, chargeType)
                  if (status.equals("1")) {
                    user_charging_month_stat.setLong(5, countMore)
                    user_charging_month_stat.setLong(12, countMore)
                    user_charging_month_stat.setDouble(9, sumMoney)
                    user_charging_month_stat.setDouble(13, sumMoney)
                    user_charging_month_stat.setLong(10, sumFlow)
                    user_charging_month_stat.setLong(14, sumFlow)
                    user_charging_month_stat.setLong(11, sumCount)
                    user_charging_month_stat.setLong(15, sumCount)
                    user_charging_month_stat.setLong(8, charge_id)
                    user_charging_month_stat.setLong(16, charge_id)
                  }
                  //处理日期
                  user_charging_month_stat.setString(6, accessmonth)
                  val sqlnum_user_charging_month_stat = user_charging_month_stat.executeUpdate()
                  if (sqlnum_user_charging_month_stat == 0) {
                    errorlogger.error("user charging month stat insert mysql error")
                  }
                  //4，mysql 项目计费统计天、月
                  //项目天
                  val project_charging_day_stat_sql = "INSERT INTO project_charging_day(logo,project,api_id," +
                    "access_count,access_day,charge_type,charge_id,access_money,update_time,access_flow,deal_count) " +
                    "VALUES (?,?,?,?,?,?,?,?,NOW(),?,?) ON DUPLICATE KEY " +
                    "UPDATE access_count = access_count + ?,access_money=access_money+?,update_time = NOW()," +
                    "access_flow=access_flow+?,deal_count=deal_count+?,charge_id=?"
                  val project_charging_day_stat = connection.prepareStatement(project_charging_day_stat_sql)
                  project_charging_day_stat.setString(1, userLogo)
                  project_charging_day_stat.setString(2, userProject)
                  project_charging_day_stat.setLong(3, apiId)
                  project_charging_day_stat.setInt(6, chargeType)
                  if (status.equals("1")) {
                    project_charging_day_stat.setLong(4, countMore)
                    project_charging_day_stat.setLong(11, countMore)
                    project_charging_day_stat.setDouble(8, sumMoney)
                    project_charging_day_stat.setDouble(12, sumMoney)
                    project_charging_day_stat.setLong(9, sumFlow)
                    project_charging_day_stat.setLong(13, sumFlow)
                    project_charging_day_stat.setLong(10, sumCount)
                    project_charging_day_stat.setLong(14, sumCount)
                    project_charging_day_stat.setLong(7, charge_id)
                    project_charging_day_stat.setLong(15, charge_id)
                  }
                  //处理日期
                  project_charging_day_stat.setString(5, accessday)
                  val sqlnum_project_charging_day_stat = project_charging_day_stat.executeUpdate()
                  if (sqlnum_project_charging_day_stat == 0) {
                    errorlogger.error("project charging day stat insert mysql error")
                  }
                  //项目月
                  val project_charging_month_stat_sql = "INSERT INTO project_charging_month(logo,project,api_id," +
                    "access_count,access_month,charge_type,charge_id,access_money,update_time,access_flow,deal_count) " +
                    "VALUES (?,?,?,?,?,?,?,?,NOW(),?,?) ON DUPLICATE KEY " +
                    "UPDATE access_count = access_count + ?,access_money=access_money+?,update_time = NOW()," +
                    "access_flow=access_flow+?,deal_count=deal_count+?,charge_id=?"
                  val project_charging_month_stat = connection.prepareStatement(project_charging_month_stat_sql)
                  project_charging_month_stat.setString(1, userLogo)
                  project_charging_month_stat.setString(2, userProject)
                  project_charging_month_stat.setLong(3, apiId)
                  project_charging_month_stat.setInt(6, chargeType)
                  if (status.equals("1")) {
                    project_charging_month_stat.setLong(4, countMore)
                    project_charging_month_stat.setLong(11, countMore)
                    project_charging_month_stat.setDouble(8, sumMoney)
                    project_charging_month_stat.setDouble(12, sumMoney)
                    project_charging_month_stat.setLong(9, sumFlow)
                    project_charging_month_stat.setLong(13, sumFlow)
                    project_charging_month_stat.setLong(10, sumCount)
                    project_charging_month_stat.setLong(14, sumCount)
                    project_charging_month_stat.setLong(7, charge_id)
                    project_charging_month_stat.setLong(15, charge_id)
                  }
                  //处理日期
                  project_charging_month_stat.setString(5, accessmonth)
                  val sqlnum_project_charging_month_stat = project_charging_month_stat.executeUpdate()
                  if (sqlnum_project_charging_month_stat == 0) {
                    errorlogger.error("project charging month stat insert mysql error")
                  }
                }
              })
            catch {
              case e: Exception => print("exception caught: " + e);
            } finally {
              ConnectionPool.closeConnection(connection)
            }
          }
        }
        }
      })
    } catch {
      case e: Exception => infologger.error("exception caught: " + e);
    }

    ssc.start()
    ssc.awaitTermination()
  }

}

//{"api_id":1,"use_time":10,"user_logo":"","ip":"127.0.0.1","message":"succcess","in_param":"userId=1","url":"/data/getToken",
// "accessKey":"test_access_key","api_type":0,"user_id":"","user_project":"","add_time":1502608722358,"app_id":1,
// "app_code":"test_app_code","status":1}
case class AccessLog(userId: String, userLogo: String, userProject: String,
                     appId: Long, appCode: String,
                     apiId: Long, url: String, apiType: Int, inParam: String,
                     useTime: String, ip: String, addTime: String, status: Int,
                     message: String, accessKey: String, chargeType: Int,
                     accessFlow: Long, dealCount: Long
                    )


object SQLContextSingleton {
  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
