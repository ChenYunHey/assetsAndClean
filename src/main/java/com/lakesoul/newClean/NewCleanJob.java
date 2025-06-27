package com.lakesoul.newClean;

import com.lakesoul.assets.PgDeserialization;
import com.lakesoul.assets.util.SourceOptions;
import com.lakesoul.clean.CleanUtils;
import com.lakesoul.newClean.PartitionInfoRecordGets.PartitionInfo;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class NewCleanJob {
    public static int expiredTime = 60000;
    public static String host;
    private static String dbName;
    public static String userName;
    public static String passWord;
    public static int port;
    private static int splitSize;
    private static String slotName;
    private static String pluginName;
    private static String schemaList;
    public static String pgUrl;
    private static int sourceParallelism;
    private static long ontimerInterval;

    public static void main(String[] args) throws Exception {
        PgDeserialization deserialization = new PgDeserialization();
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("include.unknown.datatypes", "true");
        String[] tableList = new String[]{"public.partition_info"};
        ParameterTool parameter = ParameterTool.fromArgs(args);
        userName = parameter.get(SourceOptions.SOURCE_DB_USER.key());
        dbName = parameter.get(SourceOptions.SOURCE_DB_DB_NAME.key());
        passWord = parameter.get(SourceOptions.SOURCE_DB_PASSWORD.key());
        host = parameter.get(SourceOptions.SOURCE_DB_HOST.key());
        port = parameter.getInt(SourceOptions.SOURCE_DB_PORT.key(), SourceOptions.SOURCE_DB_PORT.defaultValue());
        slotName = parameter.get(SourceOptions.SLOT_NAME.key());
        pluginName = parameter.get(SourceOptions.PLUG_NAME.key());
        splitSize = parameter.getInt(SourceOptions.SPLIT_SIZE.key(), SourceOptions.SPLIT_SIZE.defaultValue());
        schemaList = parameter.get(SourceOptions.SCHEMA_LIST.key());
        pgUrl = parameter.get(SourceOptions.PG_URL.key());
        sourceParallelism = parameter.getInt(SourceOptions.SOURCE_PARALLELISM.key(), SourceOptions.SOURCE_PARALLELISM.defaultValue());
        ontimerInterval = parameter.getLong(SourceOptions.ONTIMER_INTERVAL.key(), 2) * 3600000;
        expiredTime = parameter.getInt(SourceOptions.DATA_EXPIRED_TIME.key(), 2) * 86400000;

        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname(host)
                        .port(port)
                        .database(dbName)
                        .schemaList(schemaList)
                        .tableList(tableList)
                        .username(userName)
                        .password(passWord)
                        .slotName(slotName)
                        .decodingPluginName(pluginName) // use pgoutput for PostgreSQL 10+
                        .deserializer(deserialization)
                        .splitSize(splitSize) // the split size of each snapshot split
                        .debeziumProperties(debeziumProperties)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> postgresParallelSource = env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
                .setParallelism(sourceParallelism);
        postgresParallelSource.map(new PartitionInfoRecordGets.metaMapper())
                .keyBy(value -> value.table_id + "/" + value.partition_desc)
                .process(new ProcessClean());

        env.execute();

    }

    public static class ProcessClean extends KeyedProcessFunction<String, PartitionInfo, String> {
        private transient MapState<String, Long> compactState;
        private transient MapState<String, PartitionInfo.WillStateValue> willState;
        private transient ValueState<Boolean> timerInitializedState;
        CleanUtils cleanUtils = new CleanUtils();

        public ProcessClean() throws SQLException {
        }


        @Override
        public void open(Configuration parameters) throws IOException {
            MapStateDescriptor<String, PartitionInfo.WillStateValue> willStateDesc = new MapStateDescriptor<>(
                    "willStateDesc",
                    String.class, PartitionInfo.WillStateValue.class
            );
            willState = getRuntimeContext().getMapState(willStateDesc);

            MapStateDescriptor<String, Long> compactStateDesc = new MapStateDescriptor<>(
                    "compactStateDesc",
                    String.class, Long.class
            );
            compactState = getRuntimeContext().getMapState(compactStateDesc);

            ValueStateDescriptor<Boolean> initDesc = new ValueStateDescriptor<>("timerInit", Boolean.class, false);
            timerInitializedState = getRuntimeContext().getState(initDesc);
        }

        @Override
        public void processElement(PartitionInfo value, KeyedProcessFunction<String, PartitionInfo, String>.Context ctx, Collector<String> out) throws Exception {
            String tableId = value.table_id;
            String partitionDesc = value.partition_desc;
            String commitOp = value.commit_op;
            long timestamp = value.timestamp;
            int version = value.version;
            List<String> snapshot = value.snapshot;
            PartitionInfo.WillStateValue willStateValue = new PartitionInfo.WillStateValue(timestamp, snapshot);

            if (!timerInitializedState.value()) {
                timerInitializedState.update(true);
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + ontimerInterval);
            }
            if (commitOp.equals("AppendCommit") || commitOp.equals("MergeCommit")) {
                if (compactState.contains(tableId + "/" + partitionDesc)) {
                    long compactTime = compactState.get(tableId + "/" + partitionDesc);
                    if (timestamp < compactTime - expiredTime) {
                        cleanUtils.deleteFileAndDataCommitInfo(snapshot, tableId, partitionDesc);
                        cleanUtils.cleanPartitionInfo(tableId, partitionDesc, version);
                    } else {
                        willState.put(tableId + "/" + partitionDesc + "/" + version, willStateValue);
                    }
                }
            } else {
                willState.put(tableId + "/" + partitionDesc + "/" + version, willStateValue);
            }
            if (commitOp.equals("CompactionCommit") || commitOp.equals("UpdateCommit")) {
                if (compactState.contains(tableId + "/" + partitionDesc)) {
                    long compactTime = compactState.get(tableId + "/" + partitionDesc);
                    if (timestamp > compactState.get(tableId + "/" + partitionDesc)) {
                        compactState.put(tableId + "/" + partitionDesc, timestamp);
                    } else {
                        if (timestamp < compactTime - expiredTime) {
                            cleanUtils.deleteFileAndDataCommitInfo(snapshot, tableId, partitionDesc);
                            cleanUtils.cleanPartitionInfo(tableId, partitionDesc, version);
                        }
                    }
                } else {
                    compactState.put(tableId + "/" + partitionDesc, timestamp);
                    willState.put(tableId + "/" + partitionDesc + "/" + version, willStateValue);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //CleanUtils cleanUtils = new CleanUtils();
            cleanUtils.cleanDiscardFile(expiredTime);

            for (Map.Entry<String, Long> entry : compactState.entries()) {
                String compactId = entry.getKey();
                Long commitTime = entry.getValue();
                long expiredThreshold = commitTime - expiredTime;

                Iterator<Map.Entry<String, PartitionInfo.WillStateValue>> willStateIterator = willState.iterator();
                while (willStateIterator.hasNext()) {
                    Map.Entry<String, PartitionInfo.WillStateValue> willStateEntry = willStateIterator.next();
                    String[] keys = willStateEntry.getKey().split("/");
                    String tableId = keys[0];
                    String partitionDesc = keys[1];
                    int version = Integer.parseInt(keys[2]);
                    List<String> snapshot = willStateEntry.getValue().snapshot;
                    String compositeKey = tableId + "/" + partitionDesc;
                    if (compositeKey.equals(compactId)) {
                        PartitionInfo.WillStateValue stateValue = willStateEntry.getValue();
                        if (stateValue.timestamp < expiredThreshold) {
                            cleanUtils.deleteFileAndDataCommitInfo(snapshot, tableId, partitionDesc);
                            cleanUtils.cleanPartitionInfo(tableId, partitionDesc, version);
                            willStateIterator.remove();
                        }
                    }
                }
            }
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + ontimerInterval);
        }
    }
}
