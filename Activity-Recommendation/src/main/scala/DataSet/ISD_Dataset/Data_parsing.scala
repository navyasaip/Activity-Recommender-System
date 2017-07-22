package DataSet.ISD_Dataset

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}

import scala.io.Source

/**
  * Created by avalj on 11/28/16.
  */
object Data_parsing {

  class Parse(file: File) extends Runnable {
    override def run(): Unit = processing(file)

    def barometer(precision: Double): Int = {
      if (precision <= 980)
        0
      else if (980 < precision && precision <= 1000)
        1
      else if (1000 < precision && precision <= 1025)
        2
      else if (1025 < precision && precision <= 1040)
        3
      else
        4
    }

    def processing(file: File): Unit = {
      Source.fromFile(file).getLines().drop(1).foreach { line =>
        val data = line.toString.replace("    ", " ").replace("   ", " ").replace("  ", " ").split(" ")
        val pressure = data(23)
        if (!pressure.contains("*")) {
          val datetime = data(2)
          val usaf = data(0)
          val wban = data(1)
          val newFile = scala.tools.nsc.io.File("./DataSet2/" + usaf + "-" + wban + "-" + datetime.substring(0, 4) + ".txt")
            .appendAll(usaf + "," + wban + "," + datetime + "," + pressure + "," + barometer(pressure.toDouble) + "\n")
        }
      }
    }
  }


  def main(args: Array[String]): Unit = {
    val files_to_reformat = new File("Pressure_data").listFiles.filter(_.getName.endsWith(".out"))
    var pool = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors() + 4)

    files_to_reformat.foreach {
      file =>
        pool.submit(new Parse(file))
        println(file.toString)
    }
    pool.shutdown()
    println("Parsing...")
    pool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
    println("done")
  }
}
