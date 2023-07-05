package com.kiran.common

object EventGeneratorFactory {
  def apply(eventType:String):EventGenerator = {
    eventType.toUpperCase() match {
      case "SALES" => new SalesEventGenerator()
      case "PROCURE" => new ProcureEventGenerator()
      case _ => null.asInstanceOf[EventGenerator]
    }
  }
}