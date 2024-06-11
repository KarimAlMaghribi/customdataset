package dataSetDataStreamComparison;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlinkGroupByTest {

    @Test
    public void testGroupByOperator() throws Exception {
        // Verwendung der ausgelagerten Testdaten
        List<Tuple3<Integer, Integer, Long>> input = readTestData("src/test/java/dataSetDataStreamComparison/testdata.txt");

        // Set up der Batch-Umgebung
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironment();
        // Set up der Stream-Umgebung
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironment();
        streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);

        if (input.isEmpty()) {
            // Falls die Eingabe leer ist, vergleichen wir zwei leere Listen
            List<Tuple3<Integer, Integer, Long>> emptyList = new ArrayList<>();
            assertEquals(emptyList, emptyList);
        } else {
            // Batch-Verarbeitung
            DataSet<Tuple3<Integer, Integer, Long>> ds1 = batchEnv.fromCollection(input);
            DataSet<Tuple2<Integer, Integer>> batchGroupResult = ds1
                    .groupBy(0)
                    .reduceGroup(new SumReducer())
                    .map(new RichMapFunction<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Integer>>() {
                        @Override
                        public Tuple2<Integer, Integer> map(Tuple3<Integer, Integer, Long> value) {
                            return new Tuple2<>(value.f0, value.f1);
                        }
                    })
                    .returns(new TypeHint<Tuple2<Integer, Integer>>() {}.getTypeInfo());
            List<Tuple2<Integer, Integer>> batchResult = batchGroupResult.collect();
            // Sortieren der Ergebnisse
            batchResult.sort((v1, v2) -> v1.f0.compareTo(v2.f0));

            // Stream-Verarbeitung
            DataStream<Tuple3<Integer, Integer, Long>> ds2 = streamEnv.fromCollection(input);
            DataStream<Tuple2<Integer, Integer>> streamGroupResult = ds2
                    .keyBy(new TupleKeySelector())
                    .process(new GroupSumProcessFunction());

            CloseableIterator<Tuple2<Integer, Integer>> iterator = streamGroupResult.executeAndCollect();
            List<Tuple2<Integer, Integer>> streamingResult = new ArrayList<>();
            iterator.forEachRemaining(streamingResult::add);

            // Sortieren der Ergebnisse
            streamingResult.sort((v1, v2) -> v1.f0.compareTo(v2.f0));

            // Ausgabe der Ergebnisse zur Überprüfung
            System.out.println("Batch Result: " + batchResult);
            System.out.println("Streaming Result: " + streamingResult);
            // Sicherstellen, dass die Ergebnisse der Batch- und Stream-Verarbeitung übereinstimmen
            assertEquals(batchResult, streamingResult);
        }
    }

    // Custom KeySelector-Klasse, um generische Typinformationen bereitzustellen
    public static class TupleKeySelector implements KeySelector<Tuple3<Integer, Integer, Long>, Integer> {
        @Override
        public Integer getKey(Tuple3<Integer, Integer, Long> value) {
            return value.f0;
        }
    }

    // Custom GroupReduceFunction zur Summierung der zweiten Werte in der Gruppe
    public static class SumReducer implements GroupReduceFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>> {
        @Override
        public void reduce(Iterable<Tuple3<Integer, Integer, Long>> values, Collector<Tuple3<Integer, Integer, Long>> out) {
            int key = 0;
            int sum = 0;
            for (Tuple3<Integer, Integer, Long> value : values) {
                key = value.f0;
                sum += value.f1;
            }
            out.collect(new Tuple3<>(key, sum, 0L));
        }
    }

    // Custom ProcessFunction zur Gruppensummierung
    public static class GroupSumProcessFunction extends KeyedProcessFunction<Integer, Tuple3<Integer, Integer, Long>, Tuple2<Integer, Integer>> {
        private transient ValueState<Integer> sumState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                    "sumState",
                    Integer.class
            );
            sumState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, Context ctx, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            Integer currentSum = sumState.value();
            if (currentSum == null) {
                currentSum = 0;
            }
            currentSum += value.f1;
            sumState.update(currentSum);

            // Setzen eines Timers, um die Ergebnisse nach einer Verzögerung zu emittieren
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            Integer currentSum = sumState.value();
            if (currentSum != null) {
                out.collect(new Tuple2<>(ctx.getCurrentKey(), currentSum));
                sumState.clear();
            }
        }
    }

    // Methode zum Einlesen der Testdaten aus einer Datei
    private List<Tuple3<Integer, Integer, Long>> readTestData(String fileName) throws Exception {
        List<Tuple3<Integer, Integer, Long>> data = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new java.io.FileInputStream(fileName), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                int f0 = Integer.parseInt(parts[0]);
                int f1 = Integer.parseInt(parts[1]);
                long f2 = Long.parseLong(parts[2]);
                data.add(Tuple3.of(f0, f1, f2));
            }
        }
        return data;
    }
}
