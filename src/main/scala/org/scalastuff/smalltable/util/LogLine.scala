package org.scalastuff.smalltable.util
import org.slf4j.Logger

sealed abstract class LogLevel private[util] ()
object INFO extends LogLevel
object WARN extends LogLevel 
object ERROR extends LogLevel

case class LogLine(level: LogLevel, message: String) {
  def ~> (logger: Logger) = level match {
    case WARN => logger.warn(message)
    case ERROR => logger.error(message)
  }
}