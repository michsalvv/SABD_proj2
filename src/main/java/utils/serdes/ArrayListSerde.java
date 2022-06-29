package utils.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

public class ArrayListSerde<T> implements Serde<ArrayList<T>> {

    private final Serializer<T> innerSerialiser;
    private final Deserializer<T> innerDeserialiser;

    public ArrayListSerde(Serde<T> inner) {
        innerSerialiser   = inner.serializer ();
        innerDeserialiser = inner.deserializer();
    }

    @Override
    public Serializer<ArrayList<T>> serializer() {
        return new Serializer<ArrayList<T>>() {
            @Override
            public byte[] serialize(String topic, ArrayList<T> data) {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                if (data != null ) {
                    final int size = data.size();
                    final DataOutputStream dos = new DataOutputStream(baos);
                    final Iterator<T> iterator = data.iterator();
                    try {
                        dos.writeInt(size);
                        while (iterator.hasNext()) {
                            final byte[] bytes = innerSerialiser.serialize(topic, iterator.next());
                            dos.writeInt(bytes.length);
                            dos.write(bytes);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException("Unable to serialize ArrayList", e);
                    }
                }
                return baos.toByteArray();
            }
        };
    }

    @Override
    public Deserializer<ArrayList<T>> deserializer() {
        return new Deserializer<ArrayList<T>>() {
            @Override
            public ArrayList<T> deserialize(String topic, byte[] data) {
                if (data == null || data.length == 0) {
                    return null;
                }

                final ArrayList<T> arrayList = new ArrayList<>();
                final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(data));

                try {
                    final int records = dataInputStream.readInt();
                    for (int i = 0; i < records; i++) {
                        final byte[] valueBytes = new byte[dataInputStream.readInt()];
                        dataInputStream.read(valueBytes);
                        arrayList.add(innerDeserialiser.deserialize(topic, valueBytes));
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Unable to deserialize ArrayList", e);
                }

                return arrayList;
            }
        };
    }
}