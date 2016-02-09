package utils

import org.joda.time.{DateTime, DateTimeZone}

object DateUtils {

  def toSqlTimestamp(year: Int, month: Int, day: Int, hour: Int, minute: Int): java.sql.Timestamp =
    new java.sql.Timestamp(new DateTime(year, month, day, hour, minute, 0, DateTimeZone.getDefault).getMillis)

  case class Range(left: DateTime, right: DateTime) {
    require(right.isAfter(left) || (left equals right))

    def lengthInMillis = this.right.getMillis - this.left.getMillis

    def startsWith(otherRange: Range): Boolean = this.left equals otherRange.left

    def endsWith(otherRange: Range): Boolean = this.right equals otherRange.right

    def isWider(otherRange: Range): Boolean = this.lengthInMillis > otherRange.lengthInMillis

    def isEmpty: Boolean = this.left equals this.right
  }
}
