package cn.pandadb.driver

class NotImplementMethodException extends Exception{
  override def getMessage: String = "Method not implemented..."
}

class UsernameOrPasswordErrorException extends Exception{
  override def getMessage: String = "username or password error..."
}