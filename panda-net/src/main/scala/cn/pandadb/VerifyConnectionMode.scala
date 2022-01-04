package cn.pandadb

object VerifyConnectionMode extends Enumeration {
  val CORRECT = Value(0, "correct")
  val ERROR = Value(1, "error")
  val EDIT = Value(2, "edit")

  val RESET_SUCCESS = Value(3, "reset success")
  val RESET_FAILED = Value(4, "reset failed")
}
