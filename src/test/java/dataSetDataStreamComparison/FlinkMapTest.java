package dataSetDataStreamComparison;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
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
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlinkMapTest {

    @Test
    public void testMapOperator() throws Exception {
        // Read the test data from the file
        List<Tuple3<Integer, Integer, Long>> input = readTestData("src/test/java/dataSetDataStreamComparison/testdata.txt");

        // Expected output after map operation (incrementing the first field by 1)
        List<Tuple3<Integer, Integer, Long>> expectedOutput = new ArrayList<>();
        for (Tuple3<Integer, Integer, Long> tuple : input) {
            expectedOutput.add(new Tuple3<>(tuple.f0 + 1, tuple.f1, tuple.f2));
        }
        expectedOutput.sort(Comparator.comparingInt((Tuple3<Integer, Integer, Long> t) -> t.f0)
                .thenComparingInt(t -> t.f1)
                .thenComparingLong(t -> t.f2));

        // Set up the Batch environment
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironment();
        // Set up the Stream environment
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironment();
        streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // Batch processing
        DataSet<Tuple3<Integer, Integer, Long>> batchInput = batchEnv.fromCollection(input);
        DataSet<Tuple3<Integer, Integer, Long>> batchMapResult = batchInput.map(new IncrementFirstField());
        List<Tuple3<Integer, Integer, Long>> batchResult = batchMapResult.collect();
        batchResult.sort(Comparator.comparingInt((Tuple3<Integer, Integer, Long> t) -> t.f0)
                .thenComparingInt(t -> t.f1)
                .thenComparingLong(t -> t.f2));

        // Stream processing
        DataStream<Tuple3<Integer, Integer, Long>> streamInput = streamEnv.fromCollection(input);
        DataStream<Tuple3<Integer, Integer, Long>> streamMapResult = streamInput.map(new IncrementFirstField());

        CloseableIterator<Tuple3<Integer, Integer, Long>> iterator = streamMapResult.executeAndCollect();
        List<Tuple3<Integer, Integer, Long>> streamingResult = new ArrayList<>();
        iterator.forEachRemaining(streamingResult::add);
        streamingResult.sort(Comparator.comparingInt((Tuple3<Integer, Integer, Long> t) -> t.f0)
                .thenComparingInt(t -> t.f1)
                .thenComparingLong(t -> t.f2));

        // Output results for verification
        System.out.println("Batch Result: " + batchResult);
        System.out.println("Streaming Result: " + streamingResult);
        // Ensure that the results from batch and stream processing match the expected output
        assertEquals(expectedOutput, batchResult);
        assertEquals(expectedOutput, streamingResult);
    }

    // Method to read test data from a file
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

    // MapFunction to increment the first field by 1
    public static class IncrementFirstField implements MapFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>> {
        @Override
        public Tuple3<Integer, Integer, Long> map(Tuple3<Integer, Integer, Long> value) {
            return new Tuple3<>(value.f0 + 1, value.f1, value.f2);
        }
    }
}
