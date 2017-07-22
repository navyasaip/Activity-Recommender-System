package DataSet.ISD_Dataset

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.{Executors, TimeUnit}

import scala.sys.process._

/**
  * Created by avalj on 11/28/16.
  */
object Reformat {

  class Process(filename: String) extends Runnable {
    override def run() = cmd_process(filename)

    def cmd_process(filename: String) {
      Process(Seq("java", "-cp", "C:\\BigData\\", "ishJava", "C:\\BigData\\DataSet2\\" + filename
        , "C:\\BigData\\Pressure_data\\" + filename + ".out")).!!
    }
  }

  def main(args: Array[String]): Unit = {
    var pool = Executors.newFixedThreadPool(100)
    val files_to_reformat = new File("DataSet2").listFiles.filter(_.getName.endsWith(""))
    files_to_reformat.foreach { file =>
      val p = Paths.get(file.toString)
      val filename = p.getFileName.toString
      println(filename)
      pool.submit(new Process(filename))
    }
    pool.shutdown()
    println("Reformating")
    pool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
    println("done")
  }
}
