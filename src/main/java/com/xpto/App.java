package com.xpto;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.xpto.model.Person;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.*;

/**
 * Hello world!
 */
public class App {

    public static final String ACCESS_KEY = System.getenv("accessKey");
    public static final String SECRET_KEY = System.getenv("secretKey");

    public static void main(String[] args) throws Exception {

        List<List<String>> columns = getDataForFile();
        MessageType schema = getSchemaForParquetFile();
        CustomParquetWriter writer = getParquetWriter(schema);


        for (List<String> column : columns) {
            System.out.println("Writing line: " + column.toArray());
            writer.write(column);
        }
        System.out.println("Finished writing Parquet file.");

        writer.close();
    }

    private static CustomParquetWriter getParquetWriter(MessageType schema) throws IOException {
        String outputFilePath = "C://output/" + System.currentTimeMillis() + ".parquet";
        File outputParquetFile = new File(outputFilePath);
        Path path = new Path(outputParquetFile.toURI().toString());
        return new CustomParquetWriter(path, schema, false, CompressionCodecName.SNAPPY);
    }

    private static MessageType getSchemaForParquetFile() throws IOException {
        File resource = new File("C:\\user.schema");
        String rawSchema = new String(Files.readAllBytes(resource.toPath()));
        return MessageTypeParser.parseMessageType(rawSchema);
    }

    private static List<List<String>> getDataForFile() {
        List<List<String>> data = new ArrayList<>();

        List<String> parquetFileItem1 = new ArrayList<>();
        parquetFileItem1.add("1");
        parquetFileItem1.add("jony");
        parquetFileItem1.add("true");
        parquetFileItem1.add("28");

        List<String> parquetFileItem2 = new ArrayList<>();
        parquetFileItem2.add("2");
        parquetFileItem2.add("Luanny");
        parquetFileItem2.add("false");
        parquetFileItem2.add("26");

        data.add(parquetFileItem1);
        data.add(parquetFileItem2);

        return data;
    }



//    public static void main(String[] args) throws IOException {
//        System.out.println(System.getenv("nome"));
//
//
//        AmazonS3 s3Client = createClient();
//
//        // Replace the following with the name of your S3 bucket and file.
//        String bucketName = "poc-parquet-jony";
//        String objectKey = "1675732442920.parquet";
//
//        // The SQL expression that you want to use for the S3 Select request.
//        String sqlExpression = "SELECT * FROM s3object";
//
//        SelectObjectContentRequest request = new SelectObjectContentRequest().withBucketName(bucketName).withKey(objectKey).withExpression(sqlExpression).withExpressionType("SQL").withInputSerialization(new com.amazonaws.services.s3.model.InputSerialization().withParquet(new com.amazonaws.services.s3.model.ParquetInput())).withOutputSerialization(new com.amazonaws.services.s3.model.OutputSerialization().withCsv(new CSVOutput()));
//
//        SelectObjectContentResult s3Object = s3Client.selectObjectContent(request);
//
//        SelectObjectContentEventStream eventStream = s3Object.getPayload();
//        InputStream in = eventStream.getRecordsInputStream();
////        List<SelectObjectContentEvent> allEvents = eventStream.getAllEvents();
//
//        Scanner scanner = new Scanner(in);
//
//        while (scanner.hasNext()) {
//            System.out.println(scanner.nextLine());
//        }
//
//    }

    public static AmazonS3 createClient() {
        // Cria objeto AWSCredentials com as informações de autenticação
        AWSCredentials credentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);

        // Usa AmazonS3ClientBuilder para criar uma conexão S3 usando as credenciais
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion("us-east-1").build();

        // Verifica se a conexão foi estabelecida corretamente
        if (s3Client != null) {
            System.out.println("Conexão com S3 estabelecida com sucesso!");
        } else {
            System.out.println("Falha ao estabelecer conexão com S3.");
        }

        return s3Client;
    }
}



