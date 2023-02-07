package com.xpto;

import com.xpto.model.Person;
import com.xpto.model.wrapper.PersonParquetWrapper;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class PersonApp {
    public static void main(String[] args) throws Exception {
        List<Person> people = createPeople();

        MessageType schema = getSchemaForParquetFile();
        ParquetWriter<Person> parquetWriter = getParquetWriter(schema);

        people.forEach(p -> {
            try {
                parquetWriter.write(p);
            } catch (IOException e) {
            }
        });
        parquetWriter.close();
    }


    private static ParquetWriter<Person> getParquetWriter(MessageType schema) throws IOException {

        String outputFilePath = "C://output/" + System.currentTimeMillis() + ".parquet";
        File outputParquetFile = new File(outputFilePath);
        Path path = new Path(outputParquetFile.toURI().toString());

        return new PersonParquetWrapper(path, new com.xpto.PersonWriteSupport(schema), ParquetWriter.DEFAULT_PAGE_SIZE).unwrap();
    }

    private static MessageType getSchemaForParquetFile() throws IOException {
        File resource = new File("C:\\user.schema");
        String rawSchema = new String(Files.readAllBytes(resource.toPath()));
        return MessageTypeParser.parseMessageType(rawSchema);
    }

    private static List<Person> createPeople() {
        List<Person> people = new ArrayList<>();
        people.add(new Person() {{
            setId(1L);
            setActive(true);
            setAge(28);
            setName("Jony");
        }});

        people.add(new Person() {{
            setId(2L);
            setActive(true);
            setAge(24);
            setName("Matheus");
        }});

        people.add(new Person() {{
            setId(3L);
            setActive(false);
            setAge(27);
            setName("Sergio");
        }});

        return people;
    }


}
