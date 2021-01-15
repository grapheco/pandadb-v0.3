package cn.pandadb

class NotImplementMethodException(msg: String) extends Exception{
  override def getMessage: String = s"${msg} Method not implemented..."
}

class UsernameOrPasswordErrorException extends Exception{
  override def getMessage: String = "username or password error..."
}

class NotValidAddressException() extends Exception{
  override def getMessage: String = "not a valid address, please check your uri..."
}

class CypherErrorException(msg: String) extends Exception{
  override def getMessage: String = msg
}