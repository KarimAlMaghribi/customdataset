package uniproject;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;

import java.util.List;

/**
 * Represents a dataset in the context of the Flink DataStream API.
 * <p>
 * This interface provides methods to transform and manipulate data using the DataStream API,
 * imitating the CustomDataSet API's functionality. Implementations of this interface are expected to be
 * immutable and thread-safe, designed to be used in a distributed data processing environment.
 */
public interface CustomDataSet<T> {
    <R> CustomDataSet<R> map(MapFunction<T, R> mapper);
    CustomDataSet<T> distinct();
    <K> CustomDataSet<T> distinct(KeySelector<T, K> keyExtractor);
    CustomDataSet<T> distinct(int... fields);
    CustomDataSet<T> distinct(String... fields);
    CustomDataSet<T> union(CustomDataSet<T> other);
    <R> CustomDataSet<R> flatMap(FlatMapFunction<T, R> flatMapper);

    <R> ConnectedStreams<T, R> join(CustomDataSet<R> other);
    CustomDataSet<T> filter(FilterFunction<T> filter);
    <K> CustomDataSet<T> groupBy(KeySelector<T, K> keySelector);
    CustomDataSet<T> groupBy(int... fields);
    CustomDataSet<T> groupBy(String... fields);
    <R> CustomDataSet<R> reduceGroup(GroupReduceFunction<T, R> reducer);
    List<T> collect() throws Exception;
}
