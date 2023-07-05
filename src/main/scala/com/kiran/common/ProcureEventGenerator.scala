package com.kiran.common

import java.util.Date
import java.util.Calendar
import java.text.SimpleDateFormat


class ProcureEventGenerator extends EventGenerator {
  
  def getEvent():String = {
    "Purchase Event" + "|" + (new SimpleDateFormat("YYYY-MM-DD HH:MM:SS")).format(Calendar.getInstance.getTime)
    }
}