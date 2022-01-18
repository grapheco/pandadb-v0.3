package org.grapheco.pandadb.net.rpc.values

trait Point extends Serializable {
  def srid: Int

  def x: Double

  def y: Double

  def z: Double
}

case class Point2D(ss: Int, xx: Double, yy: Double) extends Point with Serializable {
  override def srid: Int = ss

  override def x: Double = xx

  override def y: Double = yy

  override def z: Double = Double.NaN

  override def toString: String = s"point({srid:$srid, x:$x, y:$y})"
}

case class Point3D(ss: Int, xx: Double, yy: Double, zz: Double) extends Point with Serializable {
  override def srid: Int = ss

  override def x: Double = xx

  override def y: Double = yy

  override def z: Double = zz

  override def toString: String = s"point({srid:$srid, x:$x, y:$y, z:$z})"
}


