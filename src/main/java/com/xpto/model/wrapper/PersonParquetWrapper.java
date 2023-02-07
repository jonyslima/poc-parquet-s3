package com.xpto.model.wrapper;

import com.xpto.CustomWriteSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.Closeable;
import java.io.IOException;

public class PersonParquetWrapper implements Closeable {
    private final ParquetWriter parquetWriter;

    public PersonParquetWrapper(Path file, WriteSupport writeSupport, int pageSize) throws IOException {
        ParquetWriter.Builder parquetWriterbuilder = new ParquetWriter.Builder(file) {
            @Override
            protected ParquetWriter.Builder self() {
                return this;
            }

            @Override
            protected WriteSupport getWriteSupport(org.apache.hadoop.conf.Configuration conf) {
                return writeSupport;
            }
        };

        parquetWriterbuilder.withCompressionCodec(CompressionCodecName.SNAPPY);
        parquetWriterbuilder.withPageSize(pageSize);
        parquetWriter = parquetWriterbuilder.build();
    }

    public ParquetWriter unwrap() {
        return this.parquetWriter;
    }

    @Override
    public void close() throws IOException {
        parquetWriter.close();
    }
}
