package com.kiran.common

import java.util.Date
import java.util.Calendar
import java.text.SimpleDateFormat


class SalesEventGenerator extends EventGenerator {
  
//  val products = Array("
  
  def getEventDetails():String = {
    
    ""
  }
  
  def getEvent():String = {
    "Sale Event" + "|" + (new SimpleDateFormat("YYYY-MM-DD HH:MM:SS")).format(Calendar.getInstance.getTime)}
}