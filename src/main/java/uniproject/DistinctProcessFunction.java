package uniproject;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class DistinctProcessFunction<T> extends ProcessFunction<T, T> {
    private final TypeInformation<T> typeInfo;
    private transient ListState<T> listState;

    public DistinctProcessFunction(TypeInformation<T> typeInfo) {
        this.typeInfo = typeInfo;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        ListStateDescriptor<T> descriptor = new ListStateDescriptor<>("distinct-elements", typeInfo);
        listState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) throws Exception {
        boolean isDuplicate = false;
        for (T element : listState.get()) {
            if (element.equals(value)) {
                isDuplicate = true;
                break;
            }
        }
        if (!isDuplicate) {
            listState.add(value);
            out.collect(value);
        }
    }
}