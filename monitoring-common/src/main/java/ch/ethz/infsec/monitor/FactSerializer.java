package ch.ethz.infsec.monitor;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;
import java.util.ArrayList;

public class FactSerializer extends Serializer<Fact> implements Serializable {
    private static final long serialVersionUID = -4617203911449622409L;
    private static final byte LONG_TYPE = 1;
    private static final byte STRING_TYPE = 2;
    private static final byte DOUBLE_TYPE = 3;

    @Override
    public void write(Kryo kryo, Output output, Fact fact) {
        output.writeString(fact.getName());
        output.writeString(fact.getTimestamp());
        output.writeInt(fact.getArity(), true);
        for (Object argument : fact.getArguments()) {
            if (argument instanceof Long) {
                output.writeByte(LONG_TYPE);
                output.writeLong((Long) argument);
            } else if (argument instanceof String) {
                output.writeByte(STRING_TYPE);
                output.writeString((String) argument);
            } else if (argument instanceof Double) {
                output.writeByte(DOUBLE_TYPE);
                output.writeDouble((Double) argument);
            } else {
                throw new IllegalArgumentException("Cannot serialize argument type: " +
                        argument.getClass().getCanonicalName());
            }
        }
    }

    @Override
    public Fact read(Kryo kryo, Input input, Class<Fact> aClass) {
        final String name = input.readString();
        final String timestamp = input.readString();
        final int arity = input.readInt(true);
        final ArrayList<Object> arguments = new ArrayList<>(arity);
        for (int i = 0; i < arity; ++i) {
            final byte type = input.readByte();
            switch (type) {
                case LONG_TYPE:
                    arguments.add(input.readLong());
                    break;
                case STRING_TYPE:
                    arguments.add(input.readString());
                    break;
                case DOUBLE_TYPE:
                    arguments.add(input.readDouble());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type ID");
            }
        }
        return new Fact(name, timestamp, arguments);
    }
}
