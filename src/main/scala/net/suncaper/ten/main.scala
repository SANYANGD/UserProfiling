package net.suncaper.ten

import net.suncaper.ten.comb._
import net.suncaper.ten.basic.matching._
import net.suncaper.ten.basic.mining._
import net.suncaper.ten.basic.statistics._
import net.suncaper.ten.basic.PolicemanMaxOrder
import org.apache.log4j.{Level, Logger}

object main {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache").setLevel(Level.ERROR)

//    val gender = new Gender
//    gender.genderWrite
//
//    val job = new Job
//    job.jobWrite
//
//    val birthday = new Birthday
//    birthday.birthdayWrite
//
//    val marriage = new Marriage
//    marriage.marriageWrite
//
//    val source = new Source
//    source.sourceWrite
//
//    val bigCustomerId = new BigCustomerId
//    bigCustomerId.bigCustomerIdWrite
//
//    val lastAddressId = new LastAddressId
//    lastAddressId.lastAddressIdWrite
//
//    val qq = new QQ
//    qq.qqWrite
//
//    val goodsBought = new GoodsBought
//    goodsBought.goodsBoughtWrite
//
//    val politicalFace = new PoliticalFace
//    politicalFace.politicalFaceWrite
//
//    val nationality = new Nationality
//    nationality.nationalityFaceWrite


//    val paymodel = new PaymentModel
//    paymodel.payModelWrite
//
//    val consumptioncycle = new ConsumptionCycle
//    consumptioncycle.consumptionCycleWrite
//
//    val paidAmount = new PaidAmount
//    paidAmount.paidAmountWrite
//
//    val constellation = new Constellation
//    constellation.constellationWrite


    val rfm = new RFMModel
    rfm.rfmModelWrite
//
//    val prm = new ProductRecommendationModel
//    prm.pr()

    println(1)

//    val nationality = new Nationality
//    nationality.nationalityFaceWrite

    val policemanMaxOrder = new PolicemanMaxOrder

  }

}
