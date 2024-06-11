package dataSetDataStreamComparison;

import org.apache.flink.api.common.RuntimeExecutionMode;
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

public class FlinkUnionTest {

    @Test
    public void testUnionOperator() throws Exception {
        // Verwendung der ausgelagerten Testdaten
        List<Tuple3<Integer, Integer, Long>> input1 = readTestData("src/test/java/dataSetDataStreamComparison/testdata.txt");
        List<Tuple3<Integer, Integer, Long>> input2 = readTestData("src/test/java/dataSetDataStreamComparison/testdataForUnion.txt");

        // Set up der Batch-Umgebung
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironment();
        // Set up der Stream-Umgebung
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironment();
        streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);

        if (input1.isEmpty() && input2.isEmpty()) {
            // Falls die Eingabe leer ist, vergleichen wir zwei leere Listen
            List<Tuple3<Integer, Integer, Long>> emptyList = new ArrayList<>();
            assertEquals(emptyList, emptyList);
        } else {
            // Batch-Verarbeitung
            DataSet<Tuple3<Integer, Integer, Long>> ds1 = batchEnv.fromCollection(input1);
            DataSet<Tuple3<Integer, Integer, Long>> ds2 = batchEnv.fromCollection(input2);
            DataSet<Tuple3<Integer, Integer, Long>> batchUnionResult = ds1.union(ds2);
            List<Tuple3<Integer, Integer, Long>> batchResult = batchUnionResult.collect();
            // Sortieren der Ergebnisse
            batchResult.sort((v1, v2) -> v1.f2.compareTo(v2.f2));

            // Stream-Verarbeitung
            DataStream<Tuple3<Integer, Integer, Long>> ds3 = streamEnv.fromCollection(input1);
            DataStream<Tuple3<Integer, Integer, Long>> ds4 = streamEnv.fromCollection(input2);
            DataStream<Tuple3<Integer, Integer, Long>> streamUnionResult = ds3.union(ds4);

            CloseableIterator<Tuple3<Integer, Integer, Long>> iterator = streamUnionResult.executeAndCollect();
            List<Tuple3<Integer, Integer, Long>> streamingResult = new ArrayList<>();
            iterator.forEachRemaining(streamingResult::add);

            // Sortieren der Ergebnisse
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
}
