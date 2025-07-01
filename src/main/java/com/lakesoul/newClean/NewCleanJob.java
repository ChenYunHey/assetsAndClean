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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

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
    private static int ontimerInterval;
    private static CleanUtils cleanUtils;

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
        ontimerInterval = 60000;
        //ontimerInterval = parameter.getLong(SourceOptions.ONTIMER_INTERVAL.key(), 2) * 3600000;
        expiredTime = 60000;
        //connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/lakesoul_test", "lakesoul_test", "lakesoul_test");

        //expiredTime = parameter.getInt(SourceOptions.DATA_EXPIRED_TIME.key(), 2) * 86400000;


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
                .filter(Objects::nonNull)
                .keyBy(value -> value.table_id + "/" + value.partition_desc)
                .process(new ProcessClean(pgUrl, userName, passWord, expiredTime, ontimerInterval));

        env.execute();

    }

    public static class ProcessClean extends KeyedProcessFunction<String, PartitionInfo, String> {
        private transient MapState<String, PartitionInfo.WillStateValue> willState;
        private transient ValueState<Boolean> timerInitializedState;
        private transient MapState<String, Long> compactNewState;

        private final String pgUrl;
        private final String userName;
        private final String password;
        private final int expiredTime;
        private final int ontimerInterval;

        private transient Connection pgConnection;

        public ProcessClean(String pgUrl, String userName, String password, int expiredTime, int ontimerInterval) {
            this.pgUrl = pgUrl;
            this.userName = userName;
            this.password = password;
            this.expiredTime = expiredTime;
            this.ontimerInterval = ontimerInterval;
        }

        @Override
        public void open(Configuration parameters) throws SQLException {
            // 初始化状态变量
            MapStateDescriptor<String, PartitionInfo.WillStateValue> willStateDesc =
                    new MapStateDescriptor<>("willStateDesc", String.class, PartitionInfo.WillStateValue.class);
            willState = getRuntimeContext().getMapState(willStateDesc);

            MapStateDescriptor<String, Long> compactNewStateDesc =
                    new MapStateDescriptor<>("newCompactDesc", String.class, Long.class);
            compactNewState = getRuntimeContext().getMapState(compactNewStateDesc);

            ValueStateDescriptor<Boolean> initDesc =
                    new ValueStateDescriptor<>("timerInit", Boolean.class, false);
            timerInitializedState = getRuntimeContext().getState(initDesc);

            PGConnectionPool.init(pgUrl, userName, password);
            pgConnection = PGConnectionPool.getConnection();
            // 初始化 CleanUtils 实例
            cleanUtils = new CleanUtils();

            // 初始化 PostgreSQL 连接
//            try {
//                Class.forName("org.postgresql.Driver");
//                connection = DriverManager.getConnection(
//                        "jdbc:postgresql://localhost:5432/lakesoul_test", "lakesoul_test", "lakesoul_test");
//
//                System.out.println("✅ PostgreSQL connection established.");
//            } catch (ClassNotFoundException e) {
//                throw new RuntimeException("❌ PostgreSQL driver not found", e);
//            } catch (SQLException e) {
//                throw new RuntimeException("❌ Failed to connect to PostgreSQL: " + pgUrl, e);
//            }
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
                if (compactNewState.contains(tableId + "/" + partitionDesc)) {
                    long compactTime = compactNewState.get(tableId + "/" + partitionDesc);
                    if (timestamp < compactTime - expiredTime) {
                        cleanUtils.deleteFileAndDataCommitInfo(snapshot, tableId, partitionDesc, pgConnection);
                        cleanUtils.cleanPartitionInfo(tableId, partitionDesc, version, pgConnection);
                    } else {
                        willState.put(tableId + "/" + partitionDesc + "/" + version, willStateValue);
                    }
                }
            } else {
                willState.put(tableId + "/" + partitionDesc + "/" + version, willStateValue);
            }
            if (commitOp.equals("CompactionCommit") || commitOp.equals("UpdateCommit")) {
                if (compactNewState.contains(tableId + "/" + partitionDesc)) {
                    long compactTime = compactNewState.get(tableId + "/" + partitionDesc) ;
                    if (timestamp > compactNewState.get(tableId + "/" + partitionDesc)) {
                        compactNewState.put(tableId + "/" + partitionDesc, timestamp);
                    } else {
                        if (timestamp < compactTime - expiredTime) {
                            cleanUtils.deleteFileAndDataCommitInfo(snapshot, tableId, partitionDesc, pgConnection);
                            cleanUtils.cleanPartitionInfo(tableId, partitionDesc, version, pgConnection);
                        }
                    }
                }
                else {
                    compactNewState.put(tableId + "/" + partitionDesc, timestamp);
                    willState.put(tableId + "/" + partitionDesc + "/" + version, willStateValue);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            cleanUtils.cleanDiscardFile(expiredTime, pgConnection);
            for (Map.Entry<String, Long> entry : compactNewState.entries()) {
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
                            cleanUtils.deleteFileAndDataCommitInfo(snapshot, tableId, partitionDesc, pgConnection);
                            cleanUtils.cleanPartitionInfo(tableId, partitionDesc, version, pgConnection);
                            willStateIterator.remove();
                        }
                    }
                }
            }
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + ontimerInterval);
        }

        @Override
        public void close() throws Exception {
            if (pgConnection != null && !pgConnection.isClosed()) {
                pgConnection.close(); // HikariCP 会自动归还连接
                System.out.println("✅ PostgreSQL connection returned to pool.");
            }
        }
    }
}
