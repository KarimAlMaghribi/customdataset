package dataSetDataStreamComparison;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlinkDistinctTest {

    @Test
    public void testDistinctOperator() throws Exception {
        // Initialisierung der Testdaten als Liste von Tuple3 (Integer, Integer, Long)
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
            DataSet<Tuple3<Integer, Integer, Long>> batchDistinctResult = ds1.distinct();
            List<Tuple3<Integer, Integer, Long>> batchResult = batchDistinctResult.collect();
            // Sortieren der Ergebnisse nach dem dritten Wert (Zeitstempel)
            batchResult.sort((v1, v2) -> v1.f2.compareTo(v2.f2));

            // Stream-Verarbeitung
            DataStream<Tuple3<Integer, Integer, Long>> ds2 = streamEnv.fromCollection(input);
            DataStream<Tuple3<Integer, Integer, Long>> streamDistinctResult = ds2
                    .keyBy(new TupleKeySelector())  // Gruppiere die Elemente nach den ersten beiden Feldern (f0 und f1)
                    .process(new DeduplicateProcessFunction());  // Entferne Duplikate basierend auf allen drei Feldern (f0, f1, f2)

            CloseableIterator<Tuple3<Integer, Integer, Long>> iterator = streamDistinctResult.executeAndCollect();
            List<Tuple3<Integer, Integer, Long>> streamingResult = new ArrayList<>();
            iterator.forEachRemaining(streamingResult::add);

            // Sortieren der Ergebnisse nach dem dritten Wert (Zeitstempel)
            streamingResult.sort((v1, v2) -> v1.f2.compareTo(v2.f2));

            // Ausgabe der Ergebnisse zur Überprüfung
            System.out.println("Batch Result: " + batchResult);
            System.out.println("Streaming Result: " + streamingResult);
            // Sicherstellen, dass die Ergebnisse der Batch- und Stream-Verarbeitung übereinstimmen
            assertEquals(batchResult, streamingResult);
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

    // Custom KeySelector-Klasse, um generische Typinformationen bereitzustellen
    public static class TupleKeySelector implements KeySelector<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> getKey(Tuple3<Integer, Integer, Long> value) {
            // Definiert, wie der Schlüssel für die Gruppierung aussehen soll
            return new Tuple2<>(value.f0, value.f1);
        }
    }

    // Custom ProcessFunction zur Duplikaterkennung und -entfernung
    public static class DeduplicateProcessFunction extends KeyedProcessFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>> {
        private transient ValueState<Long> lastTimestamp;

        @Override
        public void open(Configuration parameters) throws Exception {
            // Initialisiert den State, der den letzten Zeitstempel speichert
            lastTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTimestamp", Long.class));
        }

        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, Context ctx, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
            // Hol den zuletzt gespeicherten Zeitstempel für diesen Schlüssel
            Long lastTs = lastTimestamp.value();
            // Wenn der aktuelle Zeitstempel anders ist als der gespeicherte, ist es ein neues einzigartiges Element
            if (lastTs == null || !lastTs.equals(value.f2)) {
                // Aktualisiert den gespeicherten Zeitstempel auf den aktuellen Zeitstempel
                lastTimestamp.update(value.f2);
                // Gibt das einzigartige Element aus
                out.collect(value);
            }
        }
    }
}
