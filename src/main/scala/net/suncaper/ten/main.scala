package net.suncaper.ten


import net.suncaper.ten.comb._
import net.suncaper.ten.basic.matching._
import net.suncaper.ten.basic.mining._
import net.suncaper.ten.basic.statistics._
import net.suncaper.ten.comb.PolicemanMaxOrder
import org.apache.log4j.{Level, Logger}
import java.text.SimpleDateFormat
import java.util.Date
import scala.actors.Actor
import scala.actors.threadpool.TimeUnit


object main {

  class updateDay extends Actor {

    override def act(): Unit = {

      println("updateDay " + new SimpleDateFormat("hh:mm:ss:SSS").format(new Date()))

      while (true){

        val lastAddressId = new LastAddressId
        lastAddressId.lastAddressIdWrite

        val paymodel = new PaymentModel
        paymodel.payModelWrite

        val paidAmount = new PaidAmount
        paidAmount.paidAmountWrite

        val bigCustomerId = new BigCustomerId
        bigCustomerId.bigCustomerIdWrite

        val goodsBought = new GoodsBought
        goodsBought.goodsBoughtWrite

        val rfm = new RFMModel
        rfm.rfmModelWrite

        val prm = new ProductRecommendationModel
        prm.pr()

        val policemanMaxOrder = new PolicemanMaxOrder

        //发送信息后程序停止 秒
        TimeUnit.SECONDS.sleep(60*60*24)
      }

    }

  }

  class updateWeek extends Actor {

    override def act(): Unit = {
      println("updateWeek" + new SimpleDateFormat("hh:mm:ss:SSS").format(new Date()))

      while (true){

        val consumptioncycle = new ConsumptionCycle
        consumptioncycle.consumptionCycleWrite

        //发送信息后程序停止 秒
        TimeUnit.SECONDS.sleep(60*60*24*7)

      }

    }

  }

  class updateHalfYear extends Actor {

    override def act(): Unit = {
      println("updateHalfYear " + new SimpleDateFormat("hh:mm:ss:SSS").format(new Date()))

      while (true){

        val source = new Source
        source.sourceWrite

        //发送信息后程序停止3秒
        TimeUnit.SECONDS.sleep(60*60*12*365)

      }

    }

  }

  class updateYear extends Actor {

    override def act(): Unit = {
      println("updateYear " + new SimpleDateFormat("hh:mm:ss:SSS").format(new Date()))

      while (true){

        val job = new Job
        job.jobWrite

        val marriage = new Marriage
        marriage.marriageWrite

        val qq = new QQ
        qq.qqWrite

        val politicalFace = new PoliticalFace
        politicalFace.politicalFaceWrite

        val nationality = new Nationality
        nationality.nationalityWrite

        //发送信息后程序停止3秒
        TimeUnit.SECONDS.sleep(60*60*24*365)

      }

    }

  }

  class updateNone extends Actor {

    override def act(): Unit = {
      println("updateNone " + new SimpleDateFormat("hh:mm:ss:SSS").format(new Date()))

      val gender = new Gender
      gender.genderWrite

      val birthday = new Birthday
      birthday.birthdayWrite

      val constellation = new Constellation
      constellation.constellationWrite

    }

  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache").setLevel(Level.ERROR)

    //调用start方法启动
    new updateDay().start()
    new updateWeek().start()
    new updateHalfYear().start()
    new updateYear().start()
    new updateNone().start()

//    val thDay = new MyThread
//    thDay.setName("updateDay")
//    thDay.start()
//
//    val thWeek = new MyThread
//    thWeek.setName("updateWeek")
//    thWeek.start()
//
//    val thHalfYear = new MyThread
//    thHalfYear.setName("updateHalfYear")
//    thHalfYear.start()
//
//    val thYear = new MyThread
//    thYear.setName("updateYear")
//    thYear.start()
//
//    val thNone = new MyThread
//    thNone.setName("updateNone")
//    thNone.start()

//    val lastLoginTime = new LastLoginTime
//    lastLoginTime.LastLoginTimeWrite

//    val registerTime = new RegisterTime
//    registerTime.registerTimeWrite

//    val mobile = new Mobile
//    mobile.mobileWrite

    val email = new Email
    email.emailWrite
  }

}
