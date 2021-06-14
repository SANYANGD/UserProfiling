package net.suncaper.ten

import net.suncaper.ten.basic._
import org.apache.log4j.{Level, Logger}

object main {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache").setLevel(Level.ERROR)

    val gender = new Gender
    gender.genderWrite

    val job = new Job
    job.jobWrite

  }

}
