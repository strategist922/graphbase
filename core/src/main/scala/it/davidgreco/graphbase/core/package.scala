package it.davidgreco.graphbase

import core.impl.RandomIdGenerationStrategy
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

  def toBytes(obj: Any): Array[Byte] = {
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

  implicit def toTypedBytes[T](obj: T): Array[Byte] = Bytes.add(toBytes(obj), Array(getType(obj)))

  implicit def fromTypedBytes[T](bytes: Array[Byte]): T = {
    val bl = bytes.length - 1
    val vbuffer = Bytes.head(bytes,  bl)
    (bytes.apply(bl): @switch) match {
      case 0 => vbuffer.asInstanceOf[T]
      case 1 => Bytes.toString(vbuffer).asInstanceOf[T]
      case 2 => Bytes.toLong(vbuffer).asInstanceOf[T]
      case 3 => Bytes.toInt(vbuffer).asInstanceOf[T]
      case 4 => Bytes.toShort(vbuffer).asInstanceOf[T]
      case 5 => Bytes.toFloat(vbuffer).asInstanceOf[T]
      case 6 => Bytes.toDouble(vbuffer).asInstanceOf[T]
      case 7 => Bytes.toBoolean(vbuffer).asInstanceOf[T]
      case 10 => {
        val in = new ObjectInputStream(new ByteArrayInputStream(vbuffer))
        val obj = in.readObject();
        in.close()
        obj.asInstanceOf[T]
      }
    }
  }

}