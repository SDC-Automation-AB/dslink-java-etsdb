package org.dsa.iot.etsdb.serializer;

import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.util.json.JsonArray;
import org.dsa.iot.dslink.util.json.JsonObject;
import org.dsa.iot.historian.utils.QueryData;
import org.etsdb.ByteArrayBuilder;

/**
 * @author Samuel Grenier
 */
public class ByteData extends QueryData {

    private Value value;

    private byte type;
    private byte[] bytes;

    public void setType(byte type) {
        this.type = type;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public void setValue(Value value) {
        this.value = value;
    }

    @Override
    public Value getValue() {
        if (value == null && bytes != null) {
            ByteArrayBuilder b = new ByteArrayBuilder(bytes);
            switch (type) {
                case ValueSerializer.NUMBER: {
                    type = b.get();
                    switch (type) {
                        case ValueSerializer.BYTE:
                            value = new Value(b.get());
                            break;
                        case ValueSerializer.SHORT:
                            value = new Value(b.getShort());
                            break;
                        case ValueSerializer.INT:
                            value = new Value(b.getInt());
                            break;
                        case ValueSerializer.LONG:
                            value = new Value(b.getLong());
                            break;
                        case ValueSerializer.FLOAT:
                            value = new Value(b.getFloat());
                            break;
                        case ValueSerializer.DOUBLE:
                            value = new Value(b.getDouble());
                            break;
                        default:
                            throw new RuntimeException("Unsupported type: " + type);
                    }
                    break;
                }
                case ValueSerializer.BOOL: {
                    value = new Value(b.getBoolean());
                    break;
                }
                case ValueSerializer.STRING: {
                    value = new Value(b.getString());
                    break;
                }
                case ValueSerializer.MAP: {
                    JsonObject obj = new JsonObject(b.getString());
                    value = new Value(obj);
                    break;
                }
                case ValueSerializer.ARRAY: {
                    JsonArray array = new JsonArray(b.getString());
                    value = new Value(array);
                    break;
                }
                case ValueSerializer.BINARY: {
                    int avail = b.getAvailable();
                    byte[] bytes = new byte[avail];
                    b.get(bytes, 0, avail);
                    value = new Value(bytes);
                    break;
                }
                default:
                    throw new RuntimeException("Unsupported type: " + type);
            }

            value.setTime(getTimestamp());
        }
        return value;
    }
}
