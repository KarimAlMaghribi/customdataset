package uniproject;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public class UnsortedGrouping<T> {
    private final DataStream<T> dataStream;
    private final KeySelector<T, ?> keySelector;

    public UnsortedGrouping(DataStream<T> dataStream, KeySelector<T, ?> keySelector) {
        this.dataStream = dataStream;
        this.keySelector = keySelector;
    }

    public DataStream<T> getDataStream() {
        return dataStream;
    }

    public KeySelector<T, ?> getKeySelector() {
        return keySelector;
    }
}
