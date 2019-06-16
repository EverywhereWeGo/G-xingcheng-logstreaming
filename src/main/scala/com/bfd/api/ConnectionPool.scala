package com.bfd.api

import java.sql.{Connection, ResultSet}
import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.slf4j.LoggerFactory
import java.util.Properties
import java.io.FileInputStream
import org.apache.log4j.PropertyConfigurator;

object ConnectionPool {

  private val connectionPool = {
    try{
      val prop = new Properties()
      val in = getClass().getResourceAsStream("/config.properties")
      prop.load(in)
      in.close()

      val url = prop.getProperty("mysql.url")
      val userName = prop.getProperty("mysql.userName")
      val passWord = prop.getProperty("mysql.passWord")

      println(url)
      println(userName)
      println(passWord)

      Class.forName("com.mysql.jdbc.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl(url)
      config.setUsername(userName)
      config.setPassword(passWord)
      config.setLazyInit(true)

      config.setMinConnectionsPerPartition(3)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(5)
      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(false)

      Some(new BoneCP(config))
    } catch {
      case exception:Exception=>
        print("Error in creation of connection pool"+exception.printStackTrace())
        None
    }
  }
  def getConnection:Option[Connection] ={
    connectionPool match {
      case Some(connPool) => Some(connPool.getConnection)
      case None => None
    }
  }
  def closeConnection(connection:Connection): Unit = {
    if(!connection.isClosed) {
      connection.close()

    }
  }
}