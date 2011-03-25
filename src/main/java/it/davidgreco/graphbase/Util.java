package it.davidgreco.graphbase;

import org.apache.hadoop.hbase.util.Bytes;

import com.eaio.uuid.UUID;

public class Util {

    final static byte bytearray_type = 0;
    final static byte string_type = 1;
    final static byte long_type = 2;
    final static byte int_type = 3;
    final static byte short_type = 4;
    final static byte float_type = 5;
    final static byte double_type = 6;
    final static byte boolean_type = 7;
    final static byte non_supported_type = 100;

    static byte[] generateVertexId() {
        UUID rid = new UUID();
        return Bytes.add(Bytes.toBytes(rid.getTime()), Bytes.toBytes(rid.getClockSeqAndNode()));
    }

    static byte[] generateEdgeLocalId() {
        UUID rid = new UUID();
        return Bytes.toBytes(rid.getTime());
    }

    static byte[] generateEdgeId(byte[] vertexId, long localId) {
        return generateEdgeId(vertexId, Bytes.toBytes(localId));
    }

    static byte[] generateEdgeId(byte[] vertexId, byte[] localId) {
        return Bytes.add(vertexId, localId);
    }

    static byte[] generateEdgePropertyId(String pkey, long localId) {
        return generateEdgePropertyId(pkey, Bytes.toBytes(localId));
    }

    static byte[] generateEdgePropertyId(String pkey, byte[] localId) {
        byte[] kid = Bytes.toBytes(pkey);
        return Bytes.add(kid, localId);
    }

    static EdgeIdStruct getEdgeIdStruct(byte[] edgeId) {
        EdgeIdStruct struct = new EdgeIdStruct();
        struct.vertexId = Bytes.head(edgeId, 16);;
        struct.edgeLocalId = Bytes.tail(edgeId, 8);;
        return struct;
    }

    static class EdgeIdStruct {
        byte[] vertexId;
        byte[] edgeLocalId;
    }

    static byte[] typedObjectToBytes(Object obj) {
        byte otype = non_supported_type;
        if (obj instanceof String)
            otype = string_type;
        else if (obj instanceof Long)
            otype = long_type;
        else if (obj instanceof Integer)
            otype = int_type;
        else if (obj instanceof Short)
            otype = short_type;
        else if (obj instanceof Float)
            otype = float_type;
        else if (obj instanceof Double)
            otype = double_type;
        else if (obj instanceof Boolean)
            otype = boolean_type;
        else if (obj instanceof byte[])
            otype = bytearray_type;

        byte[] otypeb = new byte[1];
        otypeb[0] = otype;
        switch (otype) {
            case bytearray_type:
                return Bytes.add(otypeb, (byte[]) obj);
            case string_type:
               return Bytes.add(otypeb, Bytes.toBytes((String) obj));
            case long_type:
                return Bytes.add(otypeb, Bytes.toBytes((Long)obj));
            case int_type:
                return Bytes.add(otypeb, Bytes.toBytes((Integer) obj));
            case short_type:
                return Bytes.add(otypeb, Bytes.toBytes((Short) obj));
            case float_type:
                return Bytes.add(otypeb, Bytes.toBytes((Float) obj));
            case double_type:
                return Bytes.add(otypeb, Bytes.toBytes((Double) obj));
            case boolean_type:
                return Bytes.add(otypeb, Bytes.toBytes((Boolean) obj));
            case non_supported_type:
                throw new RuntimeException("Non supported type");
        }
        return null;
    }

    static Object bytesToTypedObject(byte[] bvalue) {
        byte[] vbuffer = Bytes.tail(bvalue, bvalue.length-1);
        switch (bvalue[0]) {
            case bytearray_type:
                return vbuffer;
            case string_type:
               return Bytes.toString(vbuffer);
            case long_type:
                return Bytes.toLong(vbuffer);
            case int_type:
                return Bytes.toInt(vbuffer);
            case short_type:
                return Bytes.toShort(vbuffer);
            case float_type:
                return Bytes.toFloat(vbuffer);
            case double_type:
                return Bytes.toDouble(vbuffer);
            case boolean_type:
                return Bytes.toBoolean(vbuffer);
            case non_supported_type:
                throw new RuntimeException("Non supported type");
        }
        return null;
    }

}
