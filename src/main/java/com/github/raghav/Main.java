package com.github.raghav;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents;
import org.apache.hadoop.hive.ql.io.protobuf.ProtobufMessageSerDe;
import org.apache.hadoop.hive.ql.io.protobuf.ProtobufSerDe;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.tez.dag.history.logging.proto.ProtoMessageWritable;
import org.apache.tez.dag.history.logging.proto.ProtoMessageWriter;

import java.io.DataInput;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Properties;

public class Main {
    public static Configuration conf = new Configuration();
    public static ProtobufSerDe serde;

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

    public static void generateData() throws IOException {
        for (int i = 0; i < 4; i++) {
            try (FileOutputStream fos = new FileOutputStream("/tmp/out/file" + i)) {
                CodedOutputStream cos = CodedOutputStream.newInstance(fos);
                for (long j = 0; j < 1000000; j++) {
                    HiveHookEvents.HiveHookEventProto proto = HiveHookEvents.HiveHookEventProto.newBuilder()
                            .setEventType("QUERY_COMPLETED")
                            .setExecutionMode("TEZ")
                            .setHiveQueryId("hive_123456")
                            .setOperationId("ABCDEFGH")
                            .setUser("hive")
                            .setTimestamp(1234567890)
                            .build();
                    proto.writeTo(cos);
                }
                cos.flush();
            }
        }
    }

    public static void generateDataSequence() throws IOException {
        for (int i = 0; i < 4; i++) {
            ProtoMessageWriter<HiveHookEvents.HiveHookEventProto> writer = new ProtoMessageWriter<HiveHookEvents.HiveHookEventProto>(conf, new Path("/tmp/out/file" + i), HiveHookEvents.HiveHookEventProto.PARSER);
            for (long j = 0; j < 1000000; j++) {
                HiveHookEvents.HiveHookEventProto proto = HiveHookEvents.HiveHookEventProto.newBuilder()
                        .setEventType("QUERY_COMPLETED")
                        .setExecutionMode("TEZ")
                        .setHiveQueryId("hive_123456")
                        .setOperationId("ABCDEFGH")
                        .setUser("hive")
                        .setTimestamp(1234567890)
                        .build();
                writer.writeProto(proto);
            }
            writer.close();
        }
    }

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
        // generateData();
        // generateDataSequence();
        String input = args[0];
        ArrayList<FileSplit> splitList = stringToSplit(input);
        ProtoMessageWritable<HiveHookEvents.HiveHookEventProto> writable = init(HiveHookEvents.HiveHookEventProto.class, "MapFieldEntry");
        System.out.println(splitList);
        for (FileSplit split : splitList) {
            Path path = split.getPath();
            FileSystem fileSys = path.getFileSystem(conf);
            InputStream in = fileSys.open(path);
            writable.readFields((DataInput) in);
        }
    }
}