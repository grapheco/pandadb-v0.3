/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.pandadb.kernel.blob.api.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

/**
 * Created by bluejoe on 2018/11/3.
 */

object StreamUtils {
  def covertLong2ByteArray(value: Long): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    new DataOutputStream(baos).writeLong(value)
    baos.toByteArray
  }

  def convertLongArray2ByteArray(values: Array[Long]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    values.foreach(dos.writeLong(_))
    baos.toByteArray
  }

  def convertByteArray2LongArray(value: Array[Byte]): Array[Long] = {
    val baos = new DataInputStream(new ByteArrayInputStream(value))
    (1 to value.length / 8).map(x => baos.readLong()).toArray
  }
}