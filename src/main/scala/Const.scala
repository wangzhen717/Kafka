object Const {
  val INPUT_TOPIC = "Lab3-in5"
  val OUTPUT_TOPIC = "Lab3-out5"
  val STATE = "CheckInHistory6"
  val WINDOW = 3
  val BROKEN = "localhost:9092"
  val GROUPID = "group1"
}

case class CheckIn (
                     timestamp: Long,
                     city_id: Long,
                     city_name: String,
                     style: String
                   )

case class Update (city_id: Long, count: Long)
