package ch.ethz.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by renatomarroquin on 2015-11-13.
 */
object HBaseRead {
  def main(args: Array[String]) {
    var hmaster = "euler08"
    var zook = "euler08"
    var count = "false"
    if (args.length != 3)
      throw new RuntimeException("Not enough parameters")

    hmaster = args(0);
    zook = args(1);
    count = args(2);
    val sparkConf = new SparkConf().setAppName("HBaseRead")
    val sc = new SparkContext(sparkConf)

    var conf: Configuration = HBaseConfiguration.create
    val tableName = "scanks"
    conf.set("hbase.master", hmaster.concat(":").concat("60000"))
    conf.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", zook)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
//    conf.set("zookeeper.znode.parent", "/hbase-unsecure")

    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val admin = new HBaseAdmin(conf)
//    if (!admin.isTableAvailable(tableName)) {
//      val tableDesc = new HTableDescriptor(tableName)
//      admin.createTable(tableDesc)
//    }

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    if (count.equals("true")) {
      val t0 = System.nanoTime()
      println("Number of Records found : " + hBaseRDD.count())
      val t1 = System.nanoTime()
      println("Elapsed time: " + (t1 - t0)/1000000 + " ms")
    }
    else {
      var totSum: Double = 0.0
      val t0 = System.nanoTime()
      val hBaseData = hBaseRDD.map(t => t._2)
        .map(res => {
        val cell = res.getColumnLatestCell("employees".getBytes(), "s".getBytes())
        totSum += ByteBuffer.wrap(cell.getValue).getDouble
      })
//        .map(c => c.getValueArray())
//        .map(a => {
//          totSum += ByteBuffer.wrap(a).getDouble
//        })
      val hbaseCol = hBaseData.count()
      val t1 = System.nanoTime()
      println("Elapsed time: " + (t1 - t0)/1000000 + " ms")
      println("Total sum" + totSum)
//      hBaseData.foreach(println)
    }
    sc.stop()
  }
}