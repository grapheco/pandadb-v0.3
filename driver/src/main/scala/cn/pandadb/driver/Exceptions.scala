package cn.pandadb.driver

class NotImplementMethodException extends Exception{
  override def getMessage: String = "Method not implemented..."
}

class UsernameOrPasswordErrorException extends Exception{
  override def getMessage: String = "username or password error..."
}

class NotValidAddressException() extends Exception{
  override def getMessage: String = "not a valid address, please check your uri..."

}