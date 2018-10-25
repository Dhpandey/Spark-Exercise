package marklogicintegration

import java.io.{File, FileInputStream}

import com.marklogic.mapreduce.{DatabaseDocument, DocumentInputFormat, DocumentURI}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by Dheeraj on 8/5/18.
  */

object Loader extends Serializable {
  def main(args: Array[String]) {

    val sc = SparkSession
      .builder()
      .appName("SparkExercise")
      .config("spark.master", "local")
      .getOrCreate()

    val hdConf = new Configuration()
    val classLoader = getClass.getClassLoader
    val file = new File(classLoader.getResource("config.xml").getFile)
    val ipStream = new FileInputStream(file)
    hdConf.addResource(ipStream)

    val mlRDD: RDD[(DocumentURI, DatabaseDocument)] = sc.sparkContext.newAPIHadoopRDD(
      hdConf, //Configuration
      classOf[DocumentInputFormat[DatabaseDocument]], //InputFormat
      classOf[DocumentURI], //Key Class
      classOf[DatabaseDocument] //Value Class
    )

    import sc.sqlContext.implicits._

    val r: RDD[String] = mlRDD.map(x => {
      x._2.getContentAsString
    })

    sc.read.json(r.toDS()).select("address.geo.lat", "address.geo.lng", "address.zipcode")
      //      .filter("address.zipcode != '23505-1337'")
      .show

    sc.read.json(r.toDS()).select("address.geo.lat", "address.geo.lng", "address.zipcode")
      .toJavaRDD
      .saveAsTextFile("hdfs://localhost:9001/user/Dheeraj/geolocation")
  }
}
