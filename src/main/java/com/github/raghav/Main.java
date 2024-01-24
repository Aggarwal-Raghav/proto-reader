package com.github.raghav;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents;
import org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe;
import org.apache.hadoop.hive.ql.io.protobuf.ProtobufSerDe;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.tez.dag.history.logging.proto.ProtoMessageReader;
import org.apache.tez.dag.history.logging.proto.ProtoMessageWritable;
import org.apache.tez.dag.history.logging.proto.ProtoMessageWriter;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class Main {
    public static Configuration conf = new Configuration();
    public static ProtobufSerDe serde;

    // Taken this function from TestProtoMessageSerDe.java
    private static <T extends Message> ProtoMessageWritable<T> init(Class<T> clazz, String mapTypes) throws Exception {
        serde = new ProtobufMessageSerDe();
        Properties tbl = new Properties();
        tbl.setProperty("proto.class", clazz.getName());
        tbl.setProperty("proto.maptypes", mapTypes);
        serde.initialize(conf, tbl, null);

        @SuppressWarnings("rawtypes")
        Constructor<ProtoMessageWritable> cons = ProtoMessageWritable.class.getDeclaredConstructor(
                Parser.class);
        cons.setAccessible(true);
        return cons.newInstance((Parser<T>) clazz.getField("PARSER").get(null));
    }


    // Top level reader. This is similar to Tez container level reader.
    public static ProtoMessageWritable<HiveHookEvents.HiveHookEventProto> writable;

    static {
        try {
            writable = init(HiveHookEvents.HiveHookEventProto.class, "MapFieldEntry");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    // This will genereate proto data in sequence file format. This data can
    // be read from hive table also. Create large message to improve the
    // execution time
    // i -> number of files to generate
    // j -> To keep appending the row in a single file to control the file size
    public static void generateDataSequence() throws IOException {
        System.out.println("========== Started Data Generation ==========");
        for (int i = 1; i <= 4; i++) {
            ProtoMessageWriter<HiveHookEvents.HiveHookEventProto> writer =
                    new ProtoMessageWriter<HiveHookEvents.HiveHookEventProto>(conf, new Path("/tmp/data/file" + i), HiveHookEvents.HiveHookEventProto.PARSER);
            for (long j = 0; j < 100000; j++) {
                HiveHookEvents.HiveHookEventProto proto = HiveHookEvents.HiveHookEventProto.newBuilder()
                        .setEventType("QUERY_COMPLETED")
                        .setExecutionMode("TEZ")
                        .setHiveQueryId("hive_123456")
                        .setOperationId("ABCDEFGH")
                        .setUser("hive")
                        .setTimestamp(1234567890)
                        .addAllTablesRead(Arrays.asList("table1", "table2", "table3", "table4", "table5"))
                        .addAllTablesWritten(Arrays.asList("table1", "table2", "table3", "table4", "table5"))
                        .addOtherInfo(HiveHookEvents.MapFieldEntry.newBuilder().setKey("HIVE_INSTANCE_TYPE").setValue("HS2"))
                        .addOtherInfo(HiveHookEvents.MapFieldEntry.newBuilder().setKey("CLIENT_IP_ADDRESS").setValue("0.0.0.0"))
                        .addOtherInfo(HiveHookEvents.MapFieldEntry.newBuilder().setKey("HIVE_ADDRESS").setValue("0.0.0.0"))
                        .addOtherInfo(HiveHookEvents.MapFieldEntry.newBuilder().setKey("VERSION").setValue("1"))
                        .addOtherInfo(HiveHookEvents.MapFieldEntry.newBuilder().setKey("TEZ").setValue("false"))
                        .addOtherInfo(HiveHookEvents.MapFieldEntry.newBuilder().setKey("CONF").setValue("{\"AAAAAAAAA\":\"AAAAAA AAAAAAAAAAAAAAAA, AAAAAAAAAAAA,AAA(AAAAAAAAAAAAAA) AA AAAAAAAAAAA AAAA AAAA AAAAA AA AAAAAAAAAAAAAAAA, AAAAAAAAAAAA AAAAA AA AAAAAAAAAAA AAAA\",\"AAAAAAAAA\":{\"AAAAA AAAAAAAAAAAA\":{\"AAAAA-A\":{\"AAAA AAAAA\":\"AAAA\"},\"AAAAA-A\":{\"AAAAAAAAA AAAAAA\":\"AAAAA-A\"},\"AAAAA-A\":{\"AAAAAAAAA AAAAAA\":\"AAAAA-A\"}},\"AAAAA AAAAA\":{\"AAAAA-A\":{\"AAA AAAAAA\":{\"AAA AAAAAAAA AAAA:\":[{\"AAAAAAAAA\":{\"AAAAA:\":\"AAAA\",\"AAAAAAA:\":[\"AAAAAAAAAAAA\",\"AAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAA\"],\"AAAAAAAA:\":\"AAAAAAA\",\"AAAAA:\":\"AAAA\",\"AAAAAAAAAAA:\":\"AAAAA\",\"AAAAAAAAAA:\":\"AAAA\",\"AAAAAAAA\":{\"AAAAAA AAAAAAAA\":{\"AAAAAAAAAAA:\":\"AAAAAAAAAAAA (AAAA: AAAAAAAA), AAAAAAAAAAAAAA (AAAA: AAAAAAA(AA,A)), AAAAAAAAAAAAAAAA (AAAA: AAAAAA)\",\"AAAAAAAAAAAAA:\":{\"AAAAAAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAA\",\"AAAAAAA\":\"AAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAA\":\"AAAAAAAAAAAAA\",\"AAAAAAAA\":\"AAAAAAAA\",\"AAAAAAAAAAAAA\":\"AAAAAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\",\"AAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAA\",\"AAAAAAAAAAAAA\":\"AAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAA\",\"AAAAAAAAA\":\"AAAAAAAAA\",\"AAAAAAAA\":\"AAAAAAAA\",\"AAAAAAAAA\":\"AAAAAAAAA\",\"AAAAAAAAAAAAA\":\"AAAAAAAAAAAAA\",\"AAAAAAAAAAAAA\":\"AAAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\",\"AAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\",\"AAAAAAAAAAAAA\":\"AAAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAA\":\"AAAAAAAAAA\",\"AAAAAAAAAA\":\"AAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\",\"AAAAAAAA\":\"AAAAAAAA\",\"AAAAAAAAA\":\"AAAAAAAAA\",\"AAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAA\",\"AAAAAAAAAA\":\"AAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\",\"AAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAA\",\"AAAAAAAAAA\":\"AAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\",\"AAAAAAAAA\":\"AAAAAAAAA\",\"AAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAA\",\"AAAAAAAAA\":\"AAAAAAAAA\",\"AAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAA\":\"AAAAAAAAAAAAA\",\"AAAAAAAAAAAAA\":\"AAAAAAAAAAAAA\",\"AAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAA\",\"AAAAAAAAAAAAA\":\"AAAAAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\",\"AAAAAAA\":\"AAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAA\":\"AAAAAAAAAAAA\",\"AAAAAAAA\":\"AAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAA\":\"AAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAAA\":\"AAAAAAAAAAAAAAAAAA\",\"AAAAAAAAAAA\":\"AAAAAAAAAAA\"},\"AAAAAAAAAAAAAAAAA:\":[\"AAAAAAAAAAAA\",\"AAAAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAA\"],\"AAAAAAAAAA:\":\"AAAAA\",\"AAAAAAAA\":{\"AAAAA AA AAAAAAAA\":{\"AAAAAAAAAAAA:\":[\"AAA(AAAAAAAAAAAAAA)\"],\"AAAAAAAAAAAAA:\":{\"AAAAA\":\"AAAAAAAAAAAAAAAA\",\"AAAAA\":\"AAAAAAAAAAAA\"},\"AAAA:\":\"AAAAAAAAAAAAAAAA (AAAA: AAAAAA), AAAAAAAAAAAA (AAAA: AAAAAAAA)\",\"AAAA:\":\"AAAA\",\"AAAAAAAAAAAAAAAAA:\":[\"AAAAA\",\"AAAAA\",\"AAAAA\"],\"AAAAAAAAAA:\":\"AAAAA\",\"AAAAAAAA\":{\"AAAAAA AAAAAA AAAAAAAA\":{\"AAAAAAAAAAAAA:\":{\"AAA.AAAAA\":\"AAAAA\",\"AAA.AAAAA\":\"AAAAA\",\"AAAAA.AAAAA\":\"AAAAA\"},\"AAA AAAAAAAAAAA:\":\"AAAAA (AAAA: AAAAAA), AAAAA (AAAA: AAAAAAAA)\",\"AAAA AAAAA:\":\"++\",\"AAA-AAAAAA AAAAAAAAA AAAAAAA:\":\"AAAAA (AAAA: AAAAAA), AAAAA (AAAA: AAAAAAAA)\",\"AAAAA AAAAAAAAAAA:\":\"AAAAA (AAAA: AAAAAAA(AA,A))\",\"AAAAAAAAAA:\":\"AAAA\"}}}}}}}}],\"AAAAAA AAAAAAAA AAAA:\":{\"AAAAA AA AAAAAAAA\":{\"AAAAAAAAAAAA:\":[\"AAA(AAAAA.AAAAA)\"],\"AAAAAAAAAAAAA:\":{\"AAAAA\":\"AAA.AAAAA\",\"AAAAA\":\"AAA.AAAAA\"},\"AAAA:\":\"AAA.AAAAA (AAAA: AAAAAA), AAA.AAAAA (AAAA: AAAAAAAA)\",\"AAAA:\":\"AAAAAAAAAAAA\",\"AAAAAAAAAAAAAAAAA:\":[\"AAAAA\",\"AAAAA\",\"AAAAA\"],\"AAAAAAAAAA:\":\"AAAAA\",\"AAAAAAAA\":{\"AAAA AAAAAA AAAAAAAA\":{\"AAAAAAAAAA:\":\"AAAA\",\"AAAAA:\":{\"AAAAA AAAAAA:\":\"AAA.AAAAAA.AAAAAA.AAAAAA.AAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAA AAAAAA:\":\"AAA.AAAAAA.AAAAAA.AAAA.AA.AA.AAAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAA:\":\"AAA.AAAAAA.AAAAAA.AAAA.AAAAAA.AAAAAAAAAA.AAAAAAAAAAAAAAA\"},\"AAAAAAAAAA:\":\"AAAA\"}}}}}},\"AAAAA-A\":{\"AAA AAAAAA\":{\"AAA AAAAAAAA AAAA:\":[{\"AAAAAAAAA\":{\"AAAAAAA:\":[\"AAAAA\",\"AAAAA\",\"AAAAA\"],\"AAAAAAAAAA:\":\"AAAAA\",\"AAAAAAAA\":{\"AAAAAA AAAAAA AAAAAAAA\":{\"AAAAAAAAAAAAA:\":{\"AAA.AAAAAAAAAAAAAA\":\"AAAAA\",\"AAAAA.AAAAA\":\"AAAAA\",\"AAAAA.AAAAA\":\"AAAAA\"},\"AAA AAAAAAAAAAA:\":\"AAAAA (AAAA: AAAAAAA(AA,A))\",\"AAAA AAAAA:\":\"-\",\"AAAAA AAAAAAAAAAA:\":\"AAAAA (AAAA: AAAAAA), AAAAA (AAAA: AAAAAAAA)\",\"AAAAAAAAAA:\":\"AAAA\"}}}}],\"AAAAAA AAAAAAAA AAAA:\":{\"AAAAAA AAAAAAAA\":{\"AAAAAAAAAAA:\":\"AAAAA.AAAAA (AAAA: AAAAAA), AAAAA.AAAAA (AAAA: AAAAAAAA), AAA.AAAAAAAAAAAAAA (AAAA: AAAAAAA(AA,A))\",\"AAAAAAAAAAAAA:\":{\"AAAAA\":\"AAAAA.AAAAA\",\"AAAAA\":\"AAAAA.AAAAA\",\"AAAAA\":\"AAA.AAAAAAAAAAAAAA\"},\"AAAAAAAAAAAAAAAAA:\":[\"AAAAA\",\"AAAAA\",\"AAAAA\"],\"AAAAAAAAAA:\":\"AAAAA\",\"AAAAAAAA\":{\"AAAA AAAAAA AAAAAAAA\":{\"AAAAAAAAAA:\":\"AAAAA\",\"AAAAA:\":{\"AAAAA AAAAAA:\":\"AAA.AAAAAA.AAAAAA.AAAAAA.AAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAAA AAAAAA:\":\"AAA.AAAAAA.AAAAAA.AAAA.AA.AA.AAAAAAAAAAAAAAAAAAAAAAAAAAAA\",\"AAAAA:\":\"AAA.AAAAAA.AAAAAA.AAAA.AAAAAA.AAAA.AAAAAAAAAAAAAAA\"},\"AAAAAAAAAA:\":\"AAAA\"}}}}}},\"AAAAA-A\":{\"AAAAA AAAAAAAA\":{\"AAAAA:\":\"-A\",\"AAAAAAAAA AAAA:\":{\"AAAAAAAA\":{\"AAAAAAAAAA:\":\"AAAAAAAAAAAA\"}}}}}}}"))
                        .build();
                writer.writeProto(proto);
            }
            writer.close();
        }
        System.out.println("========== Completed Data Generation ==========");
    }

    // Convert the string input of file path File Split Format
    public static ArrayList<FileSplit> stringToSplit(String input) {
        ArrayList<FileSplit> splitList = new ArrayList<>();
        String[] multiplePaths = input.trim().split("\\s*,\\s*");
        for (String myStr : multiplePaths) {
            int delimiter = myStr.lastIndexOf(':');
            String[] hdfsPath = {myStr.substring(0, delimiter), myStr.substring(delimiter + 1)};
            String[] leftover = hdfsPath[1].split("\\+");
            long offset = Long.parseLong(leftover[0]);
            long bytesToRead = Long.parseLong(leftover[1]);
            Path path = new Path(hdfsPath[0]);
            String[] hosts = new String[0];
            FileSplit split = new FileSplit(path, offset, bytesToRead, hosts);
            splitList.add(split);
        }
        return splitList;
    }


    public static void main(String[] args) throws Exception {
        // Data can be generated once and is kept in /tmp/data/ dir. Commet
        // this line post data generation to save execution time
        generateDataSequence();

        // Read README.md to see how to pass input
        String input = args[0];
        ArrayList<FileSplit> splitList = stringToSplit(input);
        System.out.println("Consolidated split list: " + splitList);

        reproIssue(splitList);
    }

    public static void reproIssue(ArrayList<FileSplit> splitList) throws IOException {
        for (FileSplit split : splitList) {
            Path path = split.getPath();

            System.out.println("Processing file: " + path);

            FileSystem fileSys = path.getFileSystem(conf);

            // This Constructor is not present in Hive Code. I have modified
            // the ProtoMessageReader constructor and have provided writable
            // as the 4th arg. Writable will have CodedInputStream and will
            // ensure we use same CodedInputStream to read all the splits
            ProtoMessageReader<HiveHookEvents.HiveHookEventProto> reader = new ProtoMessageReader<HiveHookEvents.HiveHookEventProto>(conf, path, HiveHookEvents.HiveHookEventProto.PARSER, writable);

            // Start and End is to control the start and bytes to read position
            SequenceFile.Reader sfr =  new SequenceFile.Reader(fileSys, path, conf);

            if (split.getStart() > sfr.getPosition())
                sfr.sync(split.getStart());

            long end = split.getStart() + split.getLength();
            long start = sfr.getPosition();
            reader.setOffset(start);

            // Read data row by row. A costly operation and take 20-25 min to
            // run for 2 GB uncompressed data
            HiveHookEvents.HiveHookEventProto event = reader.readEvent();
            while(start < end) {
                // System.out.println(event);
                event = reader.readEvent();
                start = reader.getOffset();
            }
            if(start == end)
                System.out.println(event);
        }
    }
}