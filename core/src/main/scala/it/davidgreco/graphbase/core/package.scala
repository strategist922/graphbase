package it.davidgreco.graphbase

import org.apache.hadoop.hbase.util.Bytes
import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream, ByteArrayOutputStream}
import annotation.switch

package object core {

  val bytearray_type: Byte = 0
  val string_type: Byte = 1
  val long_type: Byte = 2
  val int_type: Byte = 3
  val short_type: Byte = 4
  val float_type: Byte = 5
  val double_type: Byte = 6
  val boolean_type: Byte = 7
  val serializable_type: Byte = 10

  implicit def toBytes(obj: Any): Array[Byte] = {
    (getType(obj): @switch) match {
      case 0 => obj.asInstanceOf[Array[Byte]]
      case 1 => Bytes.toBytes(obj.asInstanceOf[String])
      case 2 => Bytes.toBytes(obj.asInstanceOf[Long])
      case 3 => Bytes.toBytes(obj.asInstanceOf[Int])
      case 4 => Bytes.toBytes(obj.asInstanceOf[Short])
      case 5 => Bytes.toBytes(obj.asInstanceOf[Float])
      case 6 => Bytes.toBytes(obj.asInstanceOf[Double])
      case 7 => Bytes.toBytes(obj.asInstanceOf[Boolean])
      case 10 => {
        val bos = new ByteArrayOutputStream();
        val out = new ObjectOutputStream(bos);
        out.writeObject(obj)
        out.close();
        bos.toByteArray()
      }
      case _ => throw new RuntimeException("Non supported type")
    }
  }

  implicit def bytes2String(bytes: Array[Byte]): String = Bytes.toString(bytes)

  def getType(obj: Any): Byte = {
    obj match {
      case obj: Array[Byte] => 0
      case obj: String => 1
      case obj: Long => 2
      case obj: Int => 3
      case obj: Short => 4
      case obj: Float => 5
      case obj: Double => 6
      case obj: Boolean => 7
      case obj: Serializable => 10
      case _ => throw new RuntimeException("Non supported type")
    }

  }

  def toTypedBytes(obj: Any): Array[Byte] = Bytes.add(Array(getType(obj)), toBytes(obj))

  def fromTypedBytes(bytes: Array[Byte]): Any = {
    val vbuffer = Bytes.tail(bytes, bytes.length - 1)
    (bytes.apply(0): @switch) match {
      case 0 => vbuffer
      case 1 => Bytes.toString(vbuffer)
      case 2 => Bytes.toLong(vbuffer)
      case 3 => Bytes.toInt(vbuffer)
      case 4 => Bytes.toShort(vbuffer)
      case 5 => Bytes.toFloat(vbuffer)
      case 6 => Bytes.toDouble(vbuffer)
      case 7 => Bytes.toBoolean(vbuffer)
      case 10 => {
        val in = new ObjectInputStream(new ByteArrayInputStream(vbuffer))
        val obj = in.readObject();
        in.close()
        obj
      }
    }
  }

}