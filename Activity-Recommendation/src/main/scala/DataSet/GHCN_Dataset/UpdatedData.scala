package DataSet.GHCN_Dataset

import java.io.{BufferedInputStream, File, FileInputStream}
import java.net.URL
import java.util.zip.GZIPInputStream

import org.apache.commons.net.ftp.FTPClient
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.io.Source
import scala.sys.process._

/**
  * Created by avalj on 11/19/16.
  */

object UpdatedData {

  def fileDownloader(url: String, filename: String) = {
    println("Downloading Year 2016 weather data")
    new URL(url) #> new File(filename) !!
  }

  def creationDate(): String = {
    var dateModified = ""
    val file_2016 = new File("DownloadData").listFiles.filter(_.getName.endsWith(".gz"))
    file_2016.map(file => file.lastModified()).foreach { date =>
      dateModified = date.toString
    }
    dateModified // created date in milliseconds
  }

  def ftpFileCreationDate(): String = {
    // connection to FTP
    val ftpClient = new FTPClient()
    val server = "ftp.ncdc.noaa.gov"
    val port = 21
    val user = "anonymous"
    val pass = "avaljot.khurana@utdallas.edu"
    ftpClient.connect(server, port)
    ftpClient.login(user, pass)
    // use local passive mode to pass firewall
    ftpClient.enterLocalPassiveMode()
    // get details of a file or directory
    val remoteFilePath = "/pub/data/ghcn/daily/by_year/2016.csv.gz"
    var timestamp = ""
    val ftpFile = ftpClient.listFiles(remoteFilePath)
    ftpFile.foreach { file =>
      timestamp = file.getTimestamp.getTime.toString
    }
    ftpClient.logout()
    ftpClient.disconnect()
    timestamp
  }

  def downloadData(date: Int, mon: Int): Unit = {

    val updateDate = ftpFileCreationDate()
    val dtf = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss zzz yyyy")
    var updateDate_2016 = dtf.parseDateTime(updateDate)
    val todayDate = new DateTime()

    val millisec = creationDate()
    val lastDownloaded = new DateTime(millisec.toLong)

    //if today's date is not equal to date updated
    if (updateDate_2016.toLocalDate.getDayOfMonth != lastDownloaded.getDayOfMonth &&
      (todayDate.toLocalDate.getDayOfMonth != lastDownloaded.getDayOfMonth)) {
      fileDownloader("ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/2016.csv.gz", "./DownloadData/2016.csv.gz")
      println("Downloaded")

      println("Updating Texas Data Files")
      val data_2016 = Source.fromInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream("./DownloadData/2016.csv.gz"))))
      // getitng data of month>10 and date>0
      val wRdd = data_2016.getLines().map(line => (line.split(",")(0), line.split(",")(1), line.split(",")(2),
        line.split(",")(3))).filter(w => w._1.contains("USC0041") && (w._2.substring(4, 6).toInt > mon
        && w._2.substring(6, 8).toInt >= date) && (w._3.contains("TMAX") || w._3.contains("TMIN")))

      wRdd.foreach { line =>
        scala.tools.nsc.io.File("./TexasData/" + line._1 + "_" + line._3 + ".txt")
          .appendAll(line.toString().replace("(", "").replace(")", "") + "\n")
      }
    }

    else {
      println("Already downloaded and Latest Texas Data created....!!")

    }
  }
}
