package utils

object Exceptions {
  /**
   * Base exception class for all application exceptions.
   */
  class ApplicationException(val title: String, message: String) extends RuntimeException(message)

  case class NonUniqueTopicNameException(message: String) extends ApplicationException("Non Unique Topic Name", message)
  case class InvalidUserException(message: String) extends ApplicationException("Invalid User", message)
  case class InvalidAclRoleException(message: String) extends ApplicationException("Invalid Acl Role", message)
  case class ResourceNotFoundException(message: String) extends ApplicationException("Resource Not Found", message)
  case class UndefinedResourceException(message: String) extends ApplicationException("Undefined Resource", message)
  case class ExternalServiceException(message: String) extends ApplicationException("External Service Failure", message)
}
