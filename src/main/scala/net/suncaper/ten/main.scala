package net.suncaper.ten

import net.suncaper.ten.comb._
import net.suncaper.ten.basic.matching._
import org.apache.log4j.{Level, Logger}

object main {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache").setLevel(Level.ERROR)

//    val gender = new Gender
//    gender.genderWrite

//    val job = new Job
//    job.jobWrite

//    val marriage = new Marriage
//    marriage.marriageWrite

//    val source = new Source
//    source.sourceWrite

//    val bigCustomerId = new BigCustomerId
//    bigCustomerId.bigCustomerIdWrite

//    val lastAddressId = new LastAddressId
//    lastAddressId.lastAddressIdWrite

//    val birthday = new Birthday
//    birthday.birthdayWrite

//    val qq = new QQ
//    qq.qqWrite

//    val goodsBought = new GoodsBought
//    goodsBought.goodsBoughtWrite

//    val paymodel = new PaymentModel
//    paymodel.payModelWrite

//    val consumptioncycle = new ConsumptionCycle
//    consumptioncycle.consumptionCycleWrite

//    val rfm = new RFMModel
//    rfm.rfmModelWrite

//    val paidAmount = new PaidAmount
//    paidAmount.paidAmountWrite

//    val politicalFace = new PoliticalFace
//    politicalFace.politicalFaceWrite

    val nationality = new Nationality
    nationality.nationalityFaceWrite

  }

}
