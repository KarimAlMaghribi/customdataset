package dataSetDataStreamComparison;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlinkFilterTest {

    @Test
    public void testFilterOperator() throws Exception {
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
            DataSet<Tuple3<Integer, Integer, Long>> batchFilterResult = ds1.filter(new ValueFilter());
            List<Tuple3<Integer, Integer, Long>> batchResult = batchFilterResult.collect();
            // Sortieren der Ergebnisse nach dem dritten Wert (Zeitstempel)
            batchResult.sort((v1, v2) -> v1.f2.compareTo(v2.f2));

            // Stream-Verarbeitung
            DataStream<Tuple3<Integer, Integer, Long>> ds2 = streamEnv.fromCollection(input);
            DataStream<Tuple3<Integer, Integer, Long>> streamFilterResult = ds2.filter(new ValueFilter());

            CloseableIterator<Tuple3<Integer, Integer, Long>> iterator = streamFilterResult.executeAndCollect();
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

    // Custom FilterFunction, um nur Werte zu behalten, bei denen der erste Wert gerade ist
    public static class ValueFilter implements FilterFunction<Tuple3<Integer, Integer, Long>> {
        @Override
        public boolean filter(Tuple3<Integer, Integer, Long> value) {
            return value.f0 % 2 == 0;
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
