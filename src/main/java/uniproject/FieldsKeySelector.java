package uniproject;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class FieldsKeySelector<T> implements KeySelector<T, Tuple> {
    private final int[] intFields;
    private final String[] stringFields;

    public FieldsKeySelector(int... fields) {
        this.intFields = fields;
        this.stringFields = null;
    }

    public FieldsKeySelector(String... fields) {
        this.intFields = null;
        this.stringFields = fields;
    }

    @Override
    public Tuple getKey(T value) {
        if (intFields != null) {
            switch (intFields.length) {
                case 1:
                    return new Tuple1<>(getField(value, intFields[0]));
                case 2:
                    return new Tuple2<>(getField(value, intFields[0]), getField(value, intFields[1]));
                case 3:
                    return new Tuple3<>(getField(value, intFields[0]), getField(value, intFields[1]), getField(value, intFields[2]));
                default:
                    throw new UnsupportedOperationException("Currently, only up to 3 fields are supported for grouping.");
            }
        } else {
            switch (stringFields.length) {
                case 1:
                    return new Tuple1<>(getFieldByName((Tuple) value, stringFields[0]));
                case 2:
                    return new Tuple2<>(getFieldByName((Tuple) value, stringFields[0]), getFieldByName((Tuple) value, stringFields[1]));
                case 3:
                    return new Tuple3<>(getFieldByName((Tuple) value, stringFields[0]), getFieldByName((Tuple) value, stringFields[1]), getFieldByName((Tuple) value, stringFields[2]));
                default:
                    throw new UnsupportedOperationException("Currently, only up to 3 fields are supported for grouping.");
            }
        }
    }

    private Object getField(T value, int fieldIndex) {
        return ((Tuple) value).getField(fieldIndex);
    }

    private Object getFieldByName(Tuple tuple, String fieldName) {
        for (int i = 0; i < tuple.getArity(); i++) {
            if (tuple.getField(i).toString().equals(fieldName)) {
                return tuple.getField(i);
            }
        }
        throw new IllegalArgumentException("Field name " + fieldName + " not found in tuple.");
    }
}
