package dataSetDataStreamComparison;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlinkJoinTest {

    @Test
    public void testJoinOperator() throws Exception {
        // Verwendung der ausgelagerten Testdaten
        List<Tuple3<Integer, String, Long>> input1 = readTestData("src/test/java/dataSetDataStreamComparison/testdata.txt");
        List<Tuple3<Integer, Integer, Long>> input2 = readTestDataInt("src/test/java/dataSetDataStreamComparison/testdataForUnion.txt");

        // Set up der Batch-Umgebung
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironment();
        // Set up der Stream-Umgebung
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironment();
        streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);

        if (input1.isEmpty() && input2.isEmpty()) {
            // Falls die Eingabe leer ist, vergleichen wir zwei leere Listen
            List<Tuple2<String, Integer>> emptyList = new ArrayList<>();
            assertEquals(emptyList, emptyList);
        } else {
            // Batch-Verarbeitung
            DataSet<Tuple3<Integer, String, Long>> ds1 = batchEnv.fromCollection(input1);
            DataSet<Tuple3<Integer, Integer, Long>> ds2 = batchEnv.fromCollection(input2);
            DataSet<Tuple2<String, Integer>> batchJoinResult = ds1
                    .join(ds2)
                    .where(new KeySelector<Tuple3<Integer, String, Long>, Integer>() {
                        @Override
                        public Integer getKey(Tuple3<Integer, String, Long> value) {
                            return value.f0;
                        }
                    })
                    .equalTo(new KeySelector<Tuple3<Integer, Integer, Long>, Integer>() {
                        @Override
                        public Integer getKey(Tuple3<Integer, Integer, Long> value) {
                            return value.f0;
                        }
                    })
                    .with((first, second) -> new Tuple2<>(first.f1, second.f1))
                    .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
            List<Tuple2<String, Integer>> batchResult = batchJoinResult.collect();
            // Sortieren der Ergebnisse
            batchResult.sort((v1, v2) -> {
                int cmp = v1.f0.compareTo(v2.f0);
                if (cmp == 0) {
                    return v1.f1.compareTo(v2.f1);
                }
                return cmp;
            });

            // Stream-Verarbeitung
            DataStream<Tuple3<Integer, String, Long>> ds3 = streamEnv.fromCollection(input1);
            DataStream<Tuple3<Integer, Integer, Long>> ds4 = streamEnv.fromCollection(input2);
            DataStream<Tuple2<String, Integer>> streamJoinResult = ds3
                    .keyBy(new TupleKeySelectorString())
                    .connect(ds4.keyBy(new TupleKeySelectorInt()))
                    .process(new JoinProcessFunction());

            CloseableIterator<Tuple2<String, Integer>> iterator = streamJoinResult.executeAndCollect();
            List<Tuple2<String, Integer>> streamingResult = new ArrayList<>();
            iterator.forEachRemaining(streamingResult::add);

            // Sortieren der Ergebnisse
            streamingResult.sort((v1, v2) -> {
                int cmp = v1.f0.compareTo(v2.f0);
                if (cmp == 0) {
                    return v1.f1.compareTo(v2.f1);
                }
                return cmp;
            });

            // Ausgabe der Ergebnisse zur Überprüfung
            System.out.println("Batch Result: " + batchResult);
            System.out.println("Streaming Result: " + streamingResult);
            // Sicherstellen, dass die Ergebnisse der Batch- und Stream-Verarbeitung übereinstimmen
            assertEquals(batchResult, streamingResult);
        }
    }

    // Methode zum Einlesen der Testdaten aus einer Datei
    private List<Tuple3<Integer, String, Long>> readTestData(String fileName) throws Exception {
        List<Tuple3<Integer, String, Long>> data = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new java.io.FileInputStream(fileName), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                int f0 = Integer.parseInt(parts[0]);
                String f1 = parts[1];
                long f2 = Long.parseLong(parts[2]);
                data.add(Tuple3.of(f0, f1, f2));
            }
        }
        return data;
    }

    private List<Tuple3<Integer, Integer, Long>> readTestDataInt(String fileName) throws Exception {
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

    // Custom KeySelector-Klassen, um generische Typinformationen bereitzustellen
    public static class TupleKeySelectorString implements KeySelector<Tuple3<Integer, String, Long>, Integer> {
        @Override
        public Integer getKey(Tuple3<Integer, String, Long> value) {
            return value.f0;
        }
    }

    public static class TupleKeySelectorInt implements KeySelector<Tuple3<Integer, Integer, Long>, Integer> {
        @Override
        public Integer getKey(Tuple3<Integer, Integer, Long> value) {
            return value.f0;
        }
    }

    // Custom CoProcessFunction zur Verbindung der Streams
    public static class JoinProcessFunction extends CoProcessFunction<Tuple3<Integer, String, Long>, Tuple3<Integer, Integer, Long>, Tuple2<String, Integer>> {
        private List<Tuple3<Integer, String, Long>> leftBuffer = new ArrayList<>();
        private List<Tuple3<Integer, Integer, Long>> rightBuffer = new ArrayList<>();

        @Override
        public void processElement1(Tuple3<Integer, String, Long> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            leftBuffer.add(value);
            for (Tuple3<Integer, Integer, Long> right : rightBuffer) {
                if (value.f0.equals(right.f0)) {
                    out.collect(new Tuple2<>(value.f1, right.f1));
                }
            }
        }

        @Override
        public void processElement2(Tuple3<Integer, Integer, Long> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            rightBuffer.add(value);
            for (Tuple3<Integer, String, Long> left : leftBuffer) {
                if (left.f0.equals(value.f0)) {
                    out.collect(new Tuple2<>(left.f1, value.f1));
                }
            }
        }
    }
}
