package uniproject;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Implementierung eines benutzerdefinierten DataSets mit Flink-Integration.
 * Diese Klasse bietet verschiedene Methoden zur Datenmanipulation wie Mapping, Filtern, Union, usw.
 *
 * @param <T> Der Typ der Elemente im DataSet.
 */
public class CustomDataSetImpl<T> implements CustomDataSet<T> {

    private static final List<Object> collectedResults = new CopyOnWriteArrayList<>();
    final DataStream<T> internalStream;
    private final TypeInformation<T> typeInfo;

    /**
     * Konstruktor zur Initialisierung mit einer Liste von Daten.
     *
     * @param data Die Eingabeliste.
     */
    public CustomDataSetImpl(List<T> data) {
        this(StreamExecutionEnvironment.getExecutionEnvironment(), data);
    }

    /**
     * Konstruktor zur Initialisierung mit einer Liste von Daten und einer angegebenen Flink-Umgebung.
     *
     * @param env  Die Flink-Umgebung.
     * @param data Die Eingabeliste.
     */
    public CustomDataSetImpl(StreamExecutionEnvironment env, List<T> data) {
        env.setParallelism(1);
        this.typeInfo = TypeExtractor.getForObject(data.get(0));
        this.internalStream = env.fromCollection(data, typeInfo)
                .assignTimestampsAndWatermarks(createTimestampAssigner(typeInfo));
    }

    /**
     * Konstruktor zur Initialisierung mit einer Liste von Daten und Typinformationen.
     *
     * @param data     Die Eingabeliste.
     * @param typeInfo Die Typinformationen der Daten.
     */
    public CustomDataSetImpl(List<T> data, TypeInformation<T> typeInfo) {
        this(StreamExecutionEnvironment.getExecutionEnvironment(), data, typeInfo);
    }

    /**
     * Konstruktor zur Initialisierung mit einer Liste von Daten, einer Flink-Umgebung und Typinformationen.
     *
     * @param env      Die Flink-Umgebung.
     * @param data     Die Eingabeliste.
     * @param typeInfo Die Typinformationen der Daten.
     */
    public CustomDataSetImpl(StreamExecutionEnvironment env, List<T> data, TypeInformation<T> typeInfo) {
        env.setParallelism(1);
        this.internalStream = env.fromCollection(data, typeInfo)
                .assignTimestampsAndWatermarks(createTimestampAssigner(typeInfo));
        this.typeInfo = typeInfo;
    }

    /**
     * Konstruktor zur Initialisierung mit einem DataStream und Typinformationen.
     *
     * @param internalStream Der interne DataStream.
     * @param typeInfo       Die Typinformationen der Daten.
     */
    public CustomDataSetImpl(DataStream<T> internalStream, TypeInformation<T> typeInfo) {
        this.internalStream = internalStream.assignTimestampsAndWatermarks(createTimestampAssigner(typeInfo));
        this.typeInfo = typeInfo;
    }

    /**
     * Erstellt einen TimestampAssigner basierend auf den Typinformationen.
     *
     * @param typeInfo Die Typinformationen.
     * @return Der TimestampAssigner.
     */
    private AssignerWithPeriodicWatermarks<T> createTimestampAssigner(TypeInformation<T> typeInfo) {
        if (Tuple3.class.isAssignableFrom(typeInfo.getTypeClass())) {
            return new CustomTimestampAssignerForTuple3<>();
        } else if (Tuple2.class.isAssignableFrom(typeInfo.getTypeClass())) {
            return new CustomTimestampAssignerForTuple2<>();
        }
        throw new IllegalArgumentException("Unsupported tuple type: " + typeInfo.getTypeClass());
    }

    @Override
    public <R> CustomDataSet<R> map(MapFunction<T, R> mapper) {
        DataStream<R> resultStream = internalStream.map(mapper);
        return new CustomDataSetImpl<>(resultStream, resultStream.getType());
    }

    @Override
    public CustomDataSet<T> distinct() {
        DataStream<T> distinctStream = internalStream
                .keyBy(value -> value)
                .process(new DistinctProcessFunction<>(typeInfo));
        return new CustomDataSetImpl<>(distinctStream, typeInfo);
    }

    @Override
    public <K> CustomDataSet<T> distinct(KeySelector<T, K> keyExtractor) {
        DataStream<T> distinctStream = internalStream
                .keyBy(keyExtractor)
                .process(new DistinctProcessFunction<>(typeInfo));
        return new CustomDataSetImpl<>(distinctStream, typeInfo);
    }

    @Override
    public CustomDataSet<T> distinct(int... fields) {
        DataStream<T> distinctStream = internalStream
                .keyBy(new FieldsKeySelector<>(fields))
                .process(new DistinctProcessFunction<>(typeInfo));
        return new CustomDataSetImpl<>(distinctStream, typeInfo);
    }

    @Override
    public CustomDataSet<T> distinct(String... fields) {
        DataStream<T> distinctStream = internalStream
                .keyBy(new FieldsKeySelector<>(fields))
                .process(new DistinctProcessFunction<>(typeInfo));
        return new CustomDataSetImpl<>(distinctStream, typeInfo);
    }

    @Override
    public CustomDataSet<T> union(CustomDataSet<T> other) {
        DataStream<T> otherStream = ((CustomDataSetImpl<T>) other).internalStream;
        DataStream<T> resultStream = internalStream.union(otherStream);
        return new CustomDataSetImpl<>(resultStream, typeInfo);
    }

    @Override
    public <R> CustomDataSet<R> flatMap(FlatMapFunction<T, R> flatMapper) {
        DataStream<R> resultStream = internalStream.flatMap(flatMapper);
        return new CustomDataSetImpl<>(resultStream, resultStream.getType());
    }

    @Override
    public <R> ConnectedStreams<T, R> join(CustomDataSet<R> other) {
        DataStream<R> otherStream = ((CustomDataSetImpl<R>) other).internalStream;
        return internalStream.connect(otherStream);
    }

    @Override
    public CustomDataSet<T> filter(FilterFunction<T> filter) {
        DataStream<T> resultStream = internalStream.filter(filter);
        return new CustomDataSetImpl<>(resultStream, typeInfo);
    }

    @Override
    public <K> CustomDataSet<T> groupBy(KeySelector<T, K> keySelector) {
        KeyedStream<T, K> groupedStream = internalStream.keyBy(keySelector);
        return new CustomDataSetImpl<>(groupedStream, typeInfo);
    }

    @Override
    public CustomDataSet<T> groupBy(int... fields) {
        KeyedStream<T, Tuple> groupedStream = internalStream.keyBy(new FieldsKeySelector<>(fields));
        return new CustomDataSetImpl<>(groupedStream, typeInfo);
    }

    @Override
    public CustomDataSet<T> groupBy(String... fields) {
        KeyedStream<T, Tuple> groupedStream = internalStream.keyBy(new FieldsKeySelector<>(fields));
        return new CustomDataSetImpl<>(groupedStream, typeInfo);
    }

    @Override
    public <R> CustomDataSet<R> reduceGroup(GroupReduceFunction<T, R> reducer) {
        if (reducer == null) {
            throw new NullPointerException("GroupReduce function must not be null.");
        }

        TypeInformation<R> resultType = TypeExtractor.getGroupReduceReturnTypes(reducer, this.internalStream.getType(), Utils.getCallLocationName(), true);

        SingleOutputStreamOperator<R> resultStream = this.internalStream
                .keyBy(value -> value) // or use another suitable key selector
                .process(new GroupReduceProcessFunction<>(reducer, resultType))
                .returns(resultType);

        return new CustomDataSetImpl<>(resultStream, resultStream.getType());
    }

    @Override
    public List<T> collect() throws Exception {
        return executeAndCollect(internalStream);
    }

    private <E> List<E> executeAndCollect(DataStream<E> stream) throws Exception {
        collectedResults.clear();
        stream.addSink(new CollectSink<>());
        stream.getExecutionEnvironment().execute("CustomDataSet Execution");
        List<E> resultList = new ArrayList<>();
        for (Object obj : collectedResults) {
            resultList.add((E) obj);
        }
        return resultList;
    }

    private static class CollectSink<E> extends RichSinkFunction<E> {
        @Override
        public void invoke(E value, Context context) throws Exception {
            collectedResults.add(value);
            System.out.println("CollectSink: " + value);
        }
    }

    public static class CustomTimestampAssignerForTuple3<T> extends BoundedOutOfOrdernessTimestampExtractor<T> {
        public CustomTimestampAssignerForTuple3() {
            super(Time.seconds(0));
        }

        @Override
        public long extractTimestamp(T element) {
            return ((Tuple3<?, ?, Number>) element).f2.longValue();
        }
    }

    public static class CustomTimestampAssignerForTuple2<T> extends BoundedOutOfOrdernessTimestampExtractor<T> {
        public CustomTimestampAssignerForTuple2() {
            super(Time.seconds(0));
        }

        @Override
        public long extractTimestamp(T element) {
            return ((Tuple2<?, Number>) element).f1.longValue();
        }
    }

    private static class GroupReduceProcessFunction<IN, OUT> extends ProcessFunction<IN, OUT> {
        private final GroupReduceFunction<IN, OUT> reducer;
        private final TypeInformation<OUT> resultType;

        GroupReduceProcessFunction(GroupReduceFunction<IN, OUT> reducer, TypeInformation<OUT> resultType) {
            this.reducer = reducer;
            this.resultType = resultType;
        }

        @Override
        public void processElement(IN value, Context ctx, Collector<OUT> out) throws Exception {
            System.out.println("Processing element: " + value);
            List<IN> elements = new ArrayList<>();
            elements.add(value);
            reducer.reduce(elements, out);
        }
    }

    public static class CustomJoinCoProcessFunction<T, R> extends CoProcessFunction<T, R, Tuple2<T, R>> {
        private final List<T> leftElements = new ArrayList<>();
        private final List<R> rightElements = new ArrayList<>();

        @Override
        public void processElement1(T left, Context ctx, Collector<Tuple2<T, R>> out) {
            for (R right : rightElements) {
                if (isMatching(left, right)) {
                    out.collect(new Tuple2<>(left, right));
                }
            }
            leftElements.add(left);
        }

        @Override
        public void processElement2(R right, Context ctx, Collector<Tuple2<T, R>> out) {
            for (T left : leftElements) {
                if (isMatching(left, right)) {
                    out.collect(new Tuple2<>(left, right));
                }
            }
            rightElements.add(right);
        }

        private boolean isMatching(T left, R right) {
            // Implement custom join condition here
            if (left instanceof Tuple3 && right instanceof Tuple3) {
                return ((Tuple3<?, ?, ?>) left).f0.equals(((Tuple3<?, ?, ?>) right).f0);
            }
            return false;
        }
    }

}
