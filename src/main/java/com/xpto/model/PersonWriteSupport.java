
package com.xpto;

import com.xpto.model.Person;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class PersonWriteSupport extends WriteSupport<Person> {
    MessageType schema;
    RecordConsumer recordConsumer;
    List<ColumnDescriptor> cols;
    private final static List<Function<Person, Object>> FIELDS = List.of(Person::getId, Person::getName, Person::getActive, Person::getAge);

    // TODO: support specifying encodings and compression
    PersonWriteSupport(MessageType schema) {
        this.schema = schema;
        this.cols = schema.getColumns();
    }

    @Override
    public WriteContext init(Configuration config) {
        return new WriteContext(schema, new HashMap<String, String>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(Person person) {
        recordConsumer.startMessage();
        int index = 0;
        for (ColumnDescriptor col : cols) {
            String nameCol = col.getPath()[0];
            recordConsumer.startField(nameCol, index);
            Function<Person, Object> function = FIELDS.get(index);
            Object val = function.apply(person);

            switch (cols.get(index).getType()) {
                case BOOLEAN:
                    recordConsumer.addBoolean(Boolean.parseBoolean(val.toString()));
                    break;
                case FLOAT:
                    recordConsumer.addFloat(Float.parseFloat(val.toString()));
                    break;
                case DOUBLE:
                    recordConsumer.addDouble(Double.parseDouble(val.toString()));
                    break;
                case INT32:
                    recordConsumer.addInteger(Integer.parseInt(val.toString()));
                    break;
                case INT64:
                    recordConsumer.addLong(Long.parseLong(val.toString()));
                    break;
                case BINARY:
                    recordConsumer.addBinary(stringToBinary(val.toString()));
                    break;
                default:
                    throw new ParquetEncodingException(
                            "Unsupported column type: " + col.getType());
            }

            recordConsumer.endField(nameCol, index);
            index++;
        }
        recordConsumer.endMessage();

    }

    //    @Override
    public void write(List<String> values) {
        if (values.size() != cols.size()) {
            throw new ParquetEncodingException("Invalid input data. Expecting " +
                    cols.size() + " columns. Input had " + values.size() + " columns (" + cols + ") : " + values);
        }

        recordConsumer.startMessage();
        for (int i = 0; i < cols.size(); ++i) {
            String val = values.get(i);
            // val.length() == 0 indicates a NULL value.
            if (val.length() > 0) {
                recordConsumer.startField(cols.get(i).getPath()[0], i);
                switch (cols.get(i).getType()) {
                    case BOOLEAN:
                        recordConsumer.addBoolean(Boolean.parseBoolean(val));
                        break;
                    case FLOAT:
                        recordConsumer.addFloat(Float.parseFloat(val));
                        break;
                    case DOUBLE:
                        recordConsumer.addDouble(Double.parseDouble(val));
                        break;
                    case INT32:
                        recordConsumer.addInteger(Integer.parseInt(val));
                        break;
                    case INT64:
                        recordConsumer.addLong(Long.parseLong(val));
                        break;
                    case BINARY:
                        recordConsumer.addBinary(stringToBinary(val));
                        break;
                    default:
                        throw new ParquetEncodingException(
                                "Unsupported column type: " + cols.get(i).getType());
                }
                recordConsumer.endField(cols.get(i).getPath()[0], i);
            }
        }
        recordConsumer.endMessage();
    }

    private Binary stringToBinary(Object value) {
        return Binary.fromString(value.toString());
    }
}