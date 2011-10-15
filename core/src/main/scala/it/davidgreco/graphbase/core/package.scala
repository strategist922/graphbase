package it.davidgreco.graphbase

import org.apache.hadoop.hbase.util.Bytes
import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream, ByteArrayOutputStream}

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

  def toTypedBytes(obj: Any): Array[Byte] =
    obj match {
      case obj: String => Bytes.add(Array(string_type), Bytes.toBytes(obj.asInstanceOf[String]))
      case obj: Long => Bytes.add(Array(long_type), Bytes.toBytes(obj.asInstanceOf[Long]))
      case obj: Int => Bytes.add(Array(int_type), Bytes.toBytes(obj.asInstanceOf[Int]))
      case obj: Short => Bytes.add(Array(short_type), Bytes.toBytes(obj.asInstanceOf[Short]))
      case obj: Float => Bytes.add(Array(float_type), Bytes.toBytes(obj.asInstanceOf[Float]))
      case obj: Double => Bytes.add(Array(double_type), Bytes.toBytes(obj.asInstanceOf[Double]))
      case obj: Boolean => Bytes.add(Array(boolean_type), Bytes.toBytes(obj.asInstanceOf[Boolean]))
      case obj: Array[Byte] => Bytes.add(Array(bytearray_type), obj.asInstanceOf[Array[Byte]])
      case obj: Serializable => {
        val bos = new ByteArrayOutputStream();
        val out = new ObjectOutputStream(bos);
        out.writeObject(obj)
        out.close();
        Bytes.add(Array(serializable_type), bos.toByteArray())
      }
      case _ => throw new RuntimeException("Non supported type")
    }

  def fromTypedBytes(bytes: Array[Byte]): Any = {
    val vbuffer = Bytes.tail(bytes, bytes.length - 1)
    bytes.apply(0) match {
      case x if x == bytearray_type => vbuffer
      case x if x == string_type => Bytes.toString(vbuffer)
      case x if x == long_type => Bytes.toLong(vbuffer)
      case x if x == int_type => Bytes.toInt(vbuffer)
      case x if x == short_type => Bytes.toShort(vbuffer)
      case x if x == float_type => Bytes.toFloat(vbuffer)
      case x if x == double_type => Bytes.toDouble(vbuffer)
      case x if x == boolean_type => Bytes.toBoolean(vbuffer)
      case x if x == serializable_type => {
        val in = new ObjectInputStream(new ByteArrayInputStream(vbuffer))
        val obj = in.readObject();
        in.close()
        obj
      }
    }
  }

}