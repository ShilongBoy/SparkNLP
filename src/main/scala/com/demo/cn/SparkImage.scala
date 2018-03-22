package com.demo.cn

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

object SparkImage {

  def main(args: Array[String]): Unit = {
    //初始化SparkConf并设置AppName，set Master为 local用来本地测试程序
    val sparkConf = new SparkConf().setAppName("SparkImage").setMaster("local[*]")
    //初始化SparkContext
    val sparkContext = new SparkContext(sparkConf)
    //读取本地文件系统中的images图片，创建BinaryRDD imagesRDD
    val imagesRDD = sparkContext.binaryFiles("E:\\images")
    //对每个Partition，将图像数据写入HBase中
    imagesRDD.foreachPartition {
      iter => {
        //创建hbaseConfig
        val hbaseConfig = HBaseConfiguration.create()
        //写入HBase，需要客户端连接zookeeper
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
        hbaseConfig.set("hbase.zookeeper.quorum", "10.1.69.11,10.1.69.12,10.169.13")
        val connection: Connection = ConnectionFactory.createConnection(hbaseConfig);
        val tableName = "imagesTable"
        //这里需要表提前创建成功
        val table: Table = connection.getTable(TableName.valueOf(tableName))
        iter.foreach { imageFile =>
          //imageRDD中key为image文件路径，value为图像字节数组
          val tempPath = imageFile._1.split("/")
          val len = tempPath.length
          //获取图片名称
          val imageName = tempPath(len-1)
          //获取图片数据
          val imageBinary:scala.Array[scala.Byte]= imageFile._2.toArray()
          val put: Put = new Put(Bytes.toBytes(imageName))
          put.addImmutable(Bytes.toBytes("imagePath"), Bytes.toBytes("path"), Bytes.toBytes(imageFile._1))
          put.addImmutable(Bytes.toBytes("image"), Bytes.toBytes("img"),imageBinary)
          //写入HBase中
          table.put(put)
        }
        connection.close()
      }
    }

    sparkContext.stop()
  }
  //判断表是否已经存在
  def isExistTable(tableName: String, connection: Connection) {
    val admin: Admin = connection.getAdmin
    admin.tableExists(TableName.valueOf(tableName))
  }
  //创建表
  def createTable(tableName: String, columnFamilys: Array[String], connection: Connection): Unit = {
    val admin: Admin = connection.getAdmin
    if (admin.tableExists(TableName.valueOf(tableName))) {
      println("表" + tableName + "已经存在")
      return
    } else {
      val tableDesc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      for (columnFaily <- columnFamilys) {
        tableDesc.addFamily(new HColumnDescriptor(columnFaily))
      }
      admin.createTable(tableDesc)
      println("创建表成功")
    }
  }
  //向表中添加数据
  def addRow(tableName: String, row: Int, columnFaily: String, column: String, value: String, connection: Connection): Unit = {
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val put: Put = new Put(Bytes.toBytes(row))
    put.addImmutable(Bytes.toBytes(columnFaily), Bytes.toBytes(column), Bytes.toBytes(value))
    table.put(put)
  }

}
