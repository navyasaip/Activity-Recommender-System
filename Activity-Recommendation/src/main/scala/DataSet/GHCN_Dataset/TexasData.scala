package DataSet.GHCN_Dataset

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}

import scala.io.Source

/**
  * Created by avalj on 11/17/16.
  */
object TexasData {

  class Format(file: File) extends Runnable {

    override def run(): Unit = parse(file)

    def parse(file: File): Unit = {
      var fileData = Source.fromFile(file)
      for (line <- fileData.getLines()) {
        val col2 = line.split(",").map(_.trim)
        if (col2(2).contains("TMAX") || col2(2).contains("TMIN")) {
          val newFile = scala.tools.nsc.io.File("./TexasData/" + col2(0) + "_" + col2(2) + ".txt")
            .appendAll(line + "\n")
        }
      }
    }
  }


  def main(args: Array[String]): Unit = {

    val files = new File("Temp_data").listFiles.filter(_.getName.endsWith(".txt"))
    var pool = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors() * 10)
    files.foreach { file =>
      pool.submit(new Format(file))
    }
    pool.shutdown()
    println("Parsing...")
    pool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
    println("done")
  }
}
