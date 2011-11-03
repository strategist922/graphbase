package it.davidgreco.graphbase

import org.apache.hadoop.hbase.util.Bytes
import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream, ByteArrayOutputStream}
import annotation.switch
import scala.Any

package object core {

  def toBytes(t: Byte, obj: Any): Array[Byte] = {
    (t: @switch) match
    {
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

  implicit def toTypedBytes[T](obj: T): Array[Byte] = {
    val t = getType(obj)
    Bytes.add(toBytes(t, obj), Array(t))
  }

  implicit def fromTypedBytes[T](bytes: Array[Byte]): T = {
    if (bytes == null)
      return null.asInstanceOf[T]
    val bl = bytes.length - 1
    val vbuffer = Bytes.head(bytes, bl)
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