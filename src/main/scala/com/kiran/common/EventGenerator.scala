package com.kiran.common

trait EventGenerator {
  case class Event(eventType:String, eventTime:String, eventDesc:String)
  def getEvent():String
  def parseEvent(event:String):Event = {
    val eventDetails = event.split("|")
    Event(eventDetails(0), eventDetails(1),eventDetails(2))
  }
}