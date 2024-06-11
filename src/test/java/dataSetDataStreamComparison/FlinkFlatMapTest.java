package dataSetDataStreamComparison;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlinkFlatMapTest {

    @Test
    public void testFlatMapOperator() throws Exception {
        // Read the test data from the file
        List<Tuple3<Integer, Integer, Long>> input = readTestData("src/test/java/dataSetDataStreamComparison/testdata.txt");

        // Expected output after flatMap operation (splitting into individual elements with count 1)
        List<Tuple2<String, Integer>> expectedOutput = new ArrayList<>();
        for (Tuple3<Integer, Integer, Long> tuple : input) {
            expectedOutput.add(new Tuple2<>(tuple.f0.toString(), 1));
            expectedOutput.add(new Tuple2<>(tuple.f1.toString(), 1));
            expectedOutput.add(new Tuple2<>(tuple.f2.toString(), 1));
        }
        expectedOutput.sort((v1, v2) -> v1.f0.compareTo(v2.f0));

        // Set up the Batch environment
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createLocalEnvironment();
        // Set up the Stream environment
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironment();
        streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // Batch processing
        DataSet<Tuple3<Integer, Integer, Long>> batchInput = batchEnv.fromCollection(input);
        DataSet<Tuple2<String, Integer>> batchFlatMapResult = batchInput.flatMap(new Tokenizer());
        List<Tuple2<String, Integer>> batchResult = batchFlatMapResult.collect();
        batchResult.sort((v1, v2) -> v1.f0.compareTo(v2.f0));

        // Stream processing
        DataStream<Tuple3<Integer, Integer, Long>> streamInput = streamEnv.fromCollection(input);
        DataStream<Tuple2<String, Integer>> streamFlatMapResult = streamInput.flatMap(new Tokenizer());

        CloseableIterator<Tuple2<String, Integer>> iterator = streamFlatMapResult.executeAndCollect();
        List<Tuple2<String, Integer>> streamingResult = new ArrayList<>();
        iterator.forEachRemaining(streamingResult::add);
        streamingResult.sort((v1, v2) -> v1.f0.compareTo(v2.f0));

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

    // FlatMapFunction to split each Tuple3 into individual elements with count 1
    public static class Tokenizer implements FlatMapFunction<Tuple3<Integer, Integer, Long>, Tuple2<String, Integer>> {
        @Override
        public void flatMap(Tuple3<Integer, Integer, Long> value, Collector<Tuple2<String, Integer>> out) {
            out.collect(new Tuple2<>(value.f0.toString(), 1));
            out.collect(new Tuple2<>(value.f1.toString(), 1));
            out.collect(new Tuple2<>(value.f2.toString(), 1));
        }
    }
}
