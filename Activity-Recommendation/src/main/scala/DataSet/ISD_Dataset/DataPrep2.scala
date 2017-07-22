package DataSet.ISD_Dataset

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.{HttpURLConnection, URL}
import java.nio.file.Paths
import java.util.concurrent.{Executors, TimeUnit}
import java.util.zip.GZIPInputStream

import org.apache.commons.io.{FileUtils, FilenameUtils}

import scala.io.Source

/**
  * Created by avalj on 11/23/16.
  */
object DataPrep2 {

  class Download_File(url: String, filename: String) extends Runnable {

    def fileDownloader(url: String, filename: String): Any = {
      val url_to_download = "https://www1.ncdc.noaa.gov" + url
      var con: HttpURLConnection = null
      try {
        HttpURLConnection.setFollowRedirects(false)
        con = new URL(url_to_download).openConnection.asInstanceOf[HttpURLConnection]
        con.setRequestMethod("HEAD")
        con.setConnectTimeout(200 * 1000)
      }
      catch {
        case e: Exception => println("First part exception caught: " + e)
      }
      try {
        if (con.getResponseCode == HttpURLConnection.HTTP_OK) {
          //          val createUrl = new URL(url_to_download)
          //          new URL(url_to_download) #> new File(filename) !!
          FileUtils.copyURLToFile(new URL(url_to_download), new File(filename), 150 * 1000, 200 * 1000)
        }
        else {
          //println("not found: " + url_to_download)
        }
      }
      catch {
        case e: Exception => println("Second part exception caught: " + url_to_download + e)
      }
    }

    override def run(): Unit = fileDownloader(url, filename)
  }

  class UnZip(file_zip: File) extends Runnable {

    override def run(): Unit = gZip(file_zip)

    def gZip(file_zip: File): Unit = {
      val p = Paths.get(file_zip.toString)
      val zipedFile = p.getFileName.toString
      val fileNameWithOutExt = FilenameUtils.removeExtension(zipedFile)
      // name of the file
      val output_file = "./DataSet2/" + fileNameWithOutExt
      var buffer = new Array[Byte](1024)
      val gzin = new GZIPInputStream(new FileInputStream(file_zip))

      val out = new FileOutputStream(output_file)
      var len: Int = 0
      while ((len = gzin.read(buffer)) != -1) {
        out.write(buffer, 0, len)
      }
      gzin.close()
      out.close()
    }
  }


  def main(args: Array[String]): Unit = {
    var pool = Executors.newFixedThreadPool(100)
    val file = Source.fromFile("./ISD-Inv.txt")
    val parsedData = file.getLines().filter(line => line.contains("US TX"))

    parsedData.foreach {
      line =>
        val parts = line.split(" ")
        for (year <- 2000 to 2016) {
          val url = "/pub/data/noaa/" + year + "/" + parts(0) + "-" + parts(1) + "-" + year + ".gz"
          val loc = "./Pressure_Data/" + parts(0) + "-" + parts(1) + "-" + year + ".gz"
          pool.submit(new Download_File(url, loc))
          println(year)
        }
    }
    pool.shutdown()
    println("Downloading")
    pool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
    println("Downloading done")

    // Un compressing
    pool = Executors.newFixedThreadPool(100)
    val files_to_unzip = new File("Pressure_Data").listFiles.filter(_.getName.endsWith(".gz"))
    files_to_unzip.foreach {
      file_zip =>
        pool.submit(new UnZip(file_zip))
    }
    pool.shutdown()
    println("Decompressing......")
    pool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
    println("Decompressing done")
  }
}
