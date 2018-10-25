package utils


case class AccessLogs(ipAddr: Option[String] = None, clientID: String, userId: String,
                      dateTime: String, method: String, endPoint: String,
                      protocol: String, responseCode: Long, contentSize: Long) {
  // creates constructors with setting values to attributes
}

object AccessLogs {
  val regex = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r

  def logParser(log: String): AccessLogs = {
    if (regex.pattern.matcher(log).matches()) {
      val result = regex.findFirstMatchIn(log)
      val res = result.get
      AccessLogs(Some(res.group(1)), res.group(2), res.group(3),
        res.group(4), res.group(5), res.group(6),
        res.group(7), res.group(8).toLong, res.group(9).toLong)
    } else null
  }
}