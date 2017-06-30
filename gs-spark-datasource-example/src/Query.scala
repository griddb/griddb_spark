import org.apache.spark.sql.SparkSession

object Query {
  def main(args : Array[String]):Unit = {
    val session = SparkSession.builder().getOrCreate()

    val df1 = session.read.format("com.toshiba.mwcloud.gs.spark.datasource").load("point01")
    val df2 = session.read.format("com.toshiba.mwcloud.gs.spark.datasource").load("devices")

    // Case1: Spark Scala API
    val result1 = df2.select("year").where("year > 2016")
    result1.show()

    // Case2: Spark Sql
    df1.createOrReplaceTempView("point01")
    df2.createOrReplaceTempView("devices")

    val result2 = session.sql("SELECT id, year FROM devices WHERE year > 2016")
    result2.show()
    
    val result3 = session.sql("SELECT p.timestamp, p.deviceId, d.userId, d.year FROM point01 as p, devices as d WHERE p.deviceId = d.id")
    result3.show()

  }
}
