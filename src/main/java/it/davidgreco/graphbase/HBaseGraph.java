package it.davidgreco.graphbase;

import com.tinkerpop.blueprints.pgm.*;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

public class HBaseGraph implements Graph, IndexableGraph {

    final HbaseHelper handle;

    public HBaseGraph(HBaseAdmin admin, String name) {
        this.handle = new HbaseHelper(admin, name);
    }

    @Override
    public Vertex addVertex(Object o) {
        try {
            byte[] id = Util.generateVertexId();
            HBaseVertex vertex = new HBaseVertex();
            vertex.setId(id);
            vertex.setHandle(handle);
            Put put = new Put(id);
            put.add(Bytes.toBytes(handle.vnameProperties), null, null);
            handle.vtable.put(put);
            return vertex;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Vertex getVertex(Object id) {
        try {
            Get g = new Get((byte[]) id);
            Result result = handle.vtable.get(g);

            if (result.isEmpty())
                return null;

            HBaseVertex vertex = new HBaseVertex();
            vertex.setId((byte[]) id);
            return vertex;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeVertex(Vertex vertex) {
        try {
            Delete delete = new Delete((byte[]) vertex.getId());
            handle.vtable.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<Vertex> getVertices() {
        throw new RuntimeException("Not supported");
    }

    @Override
    public Edge addEdge(Object o, Vertex outVertex, Vertex inVertex, String label) {
        RowLock lockOut = null;
        RowLock lockIn = null;
        try {
            Get gOut = new Get((byte[]) outVertex.getId());
            Result resultOut = handle.vtable.get(gOut);
            Get gIn = new Get((byte[]) inVertex.getId());
            Result resultIn = handle.vtable.get(gIn);
            if (!resultIn.isEmpty() && !resultOut.isEmpty()) {
                lockOut = handle.vtable.lockRow((byte[]) outVertex.getId());
                lockIn = handle.vtable.lockRow((byte[]) inVertex.getId());
                byte[] edgeLocalId = Util.generateEdgeLocalId();
                Put outPut = new Put((byte[]) outVertex.getId(), lockOut);
                outPut.add(Bytes.toBytes(handle.vnameOutEdges), edgeLocalId, (byte[]) inVertex.getId());
                outPut.add(Bytes.toBytes(handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", edgeLocalId), Bytes.toBytes(label));
                byte[] edgeId = Util.generateEdgeId((byte[]) outVertex.getId(), edgeLocalId);
                handle.vtable.put(outPut);

                Put inPut = new Put((byte[]) inVertex.getId(), lockIn);
                inPut.add(Bytes.toBytes(handle.vnameInEdges), edgeLocalId, edgeId);
                handle.vtable.put(inPut);

                HBaseEdge edge = new HBaseEdge();
                edge.setId(edgeId);
                edge.setOutVertex((HBaseVertex) outVertex);
                edge.setInVertex((HBaseVertex) inVertex);
                edge.setLabel(label);
                edge.setHandle(handle);
                return edge;
            } else {
                throw new RuntimeException("One or both vertexes don't exist");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (lockOut != null) {
                try {
                    handle.vtable.unlockRow(lockOut);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (lockIn != null) {
                try {
                    handle.vtable.unlockRow(lockIn);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public Edge getEdge(Object id) {
        try {
            Util.EdgeIdStruct struct = Util.getEdgeIdStruct((byte[]) id);

            byte[] outVertexId = struct.vertexId;

            Get g = new Get((byte[]) struct.vertexId);
            Result result = handle.vtable.get(g);
            if (result.isEmpty())
                return null;

            byte[] inVertexId = result.getValue(Bytes.toBytes(handle.vnameOutEdges), struct.edgeLocalId);

            if (inVertexId == null) {
                return null;
            }

            String label = Bytes.toString(result.getValue(Bytes.toBytes(handle.vnameEdgeProperties), Util.generateEdgePropertyId("label", struct.edgeLocalId)));

            HBaseEdge edge = new HBaseEdge();
            HBaseVertex outVertex = new HBaseVertex();
            outVertex.setId(outVertexId);
            HBaseVertex inVertex = new HBaseVertex();
            inVertex.setId(inVertexId);
            edge.setId((byte[]) id);
            edge.setInVertex(inVertex);
            edge.setOutVertex(outVertex);
            edge.setLabel(label);
            edge.setHandle(handle);
            return edge;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeEdge(Edge edge) {
        try {
            byte[] outVertexId = (byte[]) edge.getOutVertex().getId();
            byte[] inVertexId = (byte[]) edge.getInVertex().getId();
            Get gOut = new Get(outVertexId);
            Result resultOut = handle.vtable.get(gOut);
            Get gIn = new Get(inVertexId);
            Result resultIn = handle.vtable.get(gIn);
            if (!resultIn.isEmpty() && !resultOut.isEmpty()) {
                Util.EdgeIdStruct struct = Util.getEdgeIdStruct((byte[]) edge.getId());
                Delete delete = new Delete(gOut.getRow());
                delete.deleteColumns(Bytes.toBytes(handle.vnameOutEdges), struct.edgeLocalId);
                NavigableMap<byte[], byte[]> familyMap = resultOut.getFamilyMap(Bytes.toBytes(handle.vnameEdgeProperties));
                Set<byte[]> bkeys = familyMap.keySet();
                for (byte[] bkey : bkeys) {
                    byte[] id = Bytes.tail(bkey, 8);
                    if (Bytes.equals(id, struct.edgeLocalId)) {
                        delete.deleteColumns(Bytes.toBytes(handle.vnameEdgeProperties), bkey);
                    }
                }
                handle.vtable.delete(delete);
                delete = new Delete(gIn.getRow());
                delete.deleteColumns(Bytes.toBytes(handle.vnameInEdges), struct.edgeLocalId);
                handle.vtable.delete(delete);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<Edge> getEdges() {
        throw new RuntimeException("Not supported");
    }

    @Override
    public void clear() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public <T extends Element> Index<T> createManualIndex(String indexName, Class<T> indexClass) {
        return new HBaseManualIndex<T>(this, indexName, indexClass);
    }

    @Override
    public <T extends Element> AutomaticIndex<T> createAutomaticIndex(String s, Class<T> tClass, Set<String> strings) {
        return null;
    }

    @Override
    public <T extends Element> Index<T> getIndex(String indexName, Class<T> indexClass) {
        return new HBaseManualIndex<T>(this, indexName, indexClass);
    }

    @Override
    public Iterable<Index<? extends Element>> getIndices() {
        try {
            Iterable<String> indexInternalNames = handle.getIndexNames();
            List<Index<? extends Element>> indexes = new ArrayList<Index<? extends Element>>();
            HTableDescriptor[] tables = handle.admin.listTables();
            for(String in: indexInternalNames) {

            }
            return indexes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropIndex(String name) {
        handle.dropIndexTable(name);
    }
}