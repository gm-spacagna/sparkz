package sparkz.utils

import org.apache.log4j._

case class AppLogger(logger: org.apache.log4j.Logger) {
  def info(message: String): Unit = info(() => message)
  def info(message: () => String): Unit =
    if (Level.INFO.isGreaterOrEqual(logger.getEffectiveLevel)) logger.info(message())

  def warn(message: String): Unit = warn(() => message)
  def warn(message: () => String): Unit =
    if (Level.WARN.isGreaterOrEqual(logger.getEffectiveLevel)) logger.warn(message())

  def debug(message: String): Unit = debug(() => message)
  def debug(message: () => String): Unit =
    if (Level.DEBUG.isGreaterOrEqual(logger.getEffectiveLevel)) logger.debug(message())
}

case object AppLogger {
  def getLogger(level: Level): AppLogger = {
    val logger = org.apache.log4j.Logger.getLogger("FTB")
    logger.setLevel(level)
    AppLogger(logger)
  }

  def infoLevel(): AppLogger = getLogger(Level.INFO)
  def warnLevel(): AppLogger = getLogger(Level.WARN)
  def debugLevel(): AppLogger = getLogger(Level.DEBUG)
}
