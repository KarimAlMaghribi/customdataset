package uniproject;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class GroupReduceProcessFunction<IN, OUT> extends ProcessFunction<IN, OUT> implements ResultTypeQueryable<OUT> {
    private final GroupReduceFunction<IN, OUT> reducer;
    private final TypeInformation<OUT> resultType;

    public GroupReduceProcessFunction(GroupReduceFunction<IN, OUT> reducer, TypeInformation<OUT> resultType) {
        this.reducer = reducer;
        this.resultType = resultType;
    }

    @Override
    public void processElement(IN value, Context ctx, Collector<OUT> out) throws Exception {
        List<IN> elements = new ArrayList<>();
        elements.add(value);
        reducer.reduce(elements, out);
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return resultType;
    }

}
