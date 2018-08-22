package utils

object Exceptions {
  /**
   * Base exception class for all application exceptions.
   */
  class ApplicationException(val title: String, message: String) extends RuntimeException(message)

  case class NonUniqueTopicNameException(message: String) extends ApplicationException("Non Unique Topic Name", message)
  case class InvalidUserException(message: String) extends ApplicationException("Invalid User", message)
}
