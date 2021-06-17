package net.suncaper.ten

import net.suncaper.ten.basic.matching._
import net.suncaper.ten.basic.mining._
import net.suncaper.ten.basic.statistics._
import net.suncaper.ten.comb.PolicemanMaxOrder
import org.apache.log4j.{Level, Logger}


class MyThread extends Thread {

  override def run(){
    println("Hello, This is Thread " + Thread.currentThread().getName() )
  }

  def updateDay () = {

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

  }

  def updateWeek () = {

    val consumptioncycle = new ConsumptionCycle
    consumptioncycle.consumptionCycleWrite

  }

  def updateHalfYear () = {

    val source = new Source
    source.sourceWrite

  }

  def updateYear () = {

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


  }

  def updateNone () = {

    val gender = new Gender
    gender.genderWrite

    val birthday = new Birthday
    birthday.birthdayWrite

    val constellation = new Constellation
    constellation.constellationWrite

  }

}


object main {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache").setLevel(Level.ERROR)

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
