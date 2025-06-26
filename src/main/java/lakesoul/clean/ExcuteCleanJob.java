package lakesoul.clean;


import com.lakesoul.assets.PgDeserialization;
import com.lakesoul.assets.util.SourceOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ExcuteCleanJob {

    public static int expiredTime;
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

    public static void main(String[] args) throws Exception {
        PgDeserialization deserialization = new PgDeserialization();
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("include.unknown.datatypes", "true");
        String[] tableList = new String[]{"public.data_commit_info"};
        ParameterTool parameter = ParameterTool.fromArgs(args);
        userName = parameter.get(SourceOptions.SOURCE_DB_USER.key());
        dbName = parameter.get(SourceOptions.SOURCE_DB_DB_NAME.key());
        passWord = parameter.get(SourceOptions.SOURCE_DB_PASSWORD.key());
        host = parameter.get(SourceOptions.SOURCE_DB_HOST.key());
        port = parameter.getInt(SourceOptions.SOURCE_DB_PORT.key(),SourceOptions.SOURCE_DB_PORT.defaultValue());
        slotName = parameter.get(SourceOptions.SLOT_NAME.key());
        pluginName = parameter.get(SourceOptions.PLUG_NAME.key());
        splitSize = parameter.getInt(SourceOptions.SPLIT_SIZE.key(),SourceOptions.SPLIT_SIZE.defaultValue());
        schemaList = parameter.get(SourceOptions.SCHEMA_LIST.key());
        pgUrl = parameter.get(SourceOptions.PG_URL.key());
        sourceParallelism = parameter.getInt(SourceOptions.SOURCE_PARALLELISM.key(),SourceOptions.SOURCE_PARALLELISM.defaultValue());
        expiredTime = 3 * 60 * 1000;
//        expiredTime = parameter.getInt(SourceOptions.DATA_EXPIRED_TIME.key(),SourceOptions.DATA_EXPIRED_TIME.defaultValue()) * 12 * 60 * 1000;
//        JdbcIncrementalSource<String> postgresIncrementalSource =
//                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
//                        .hostname("localhost")
//                        .port(5432)
//                        .database("lakesoul_test")
//                        .schemaList("public")
//                        .tableList(tableList)
//                        .username("lakesoul_test")
//                        .password("lakesoul_test")
//                        .slotName("flink")
//                        .decodingPluginName("pgoutput") // use pgoutput for PostgreSQL 10+
//                        .deserializer(deserialization)
//                        .includeSchemaChanges(true) // output the schema changes as well
//                        .splitSize(2) // the split size of each snapshot split
//                        .debeziumProperties(debeziumProperties)
//                        .build();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
//
//        // Define the source data stream
//        DataStreamSource<String> postgresParallelSource = env.fromSource(
//                        postgresIncrementalSource,
//                        WatermarkStrategy.noWatermarks(),
//                        "PostgresParallelSource")
//                .setParallelism(1);

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

        SingleOutputStreamOperator<RecordGets.DataCommitInfo> mainProcess = postgresParallelSource.map(new RecordGets.metaMapper());

        mainProcess.process(new GetUsefulRecord()).keyBy(value -> value.table_id + "/"+ value.partition_desc).process(new ProcessClean());
        env.execute();
    }

    public static class ProcessClean extends KeyedProcessFunction<String, RecordGets.DataCommitInfo, String> {

        private transient MapState<String,Long> compactState;
        private transient MapState<String, RecordGets.WillStateValue> willState;


        @Override
        public void open(Configuration parameters){
            MapStateDescriptor<String, RecordGets.WillStateValue> willStateDesc = new MapStateDescriptor<>(
                    "willStateDesc",
                    String.class, RecordGets.WillStateValue.class
            );
            willState = getRuntimeContext().getMapState(willStateDesc);

            MapStateDescriptor<String, Long> compactStateDesc = new MapStateDescriptor<>(
                    "compactStateDesc",
                    String.class, Long.class
            );
            compactState = getRuntimeContext().getMapState(compactStateDesc);

        }

        @Override
        public void processElement(RecordGets.DataCommitInfo dataCommitInfo, KeyedProcessFunction<String, RecordGets.DataCommitInfo, String>.Context ctx, Collector<String> collector) throws Exception {
            String commitOp = dataCommitInfo.commit_op;
            long commitTime = dataCommitInfo.commit_time;
            String tableId = dataCommitInfo.table_id;
            String commitId = dataCommitInfo.commit_id;
            String partitionDesc = dataCommitInfo.partition_desc;
            List<String> filePaths = dataCommitInfo.filePaths;
            CleanUtils cleanUtils = new CleanUtils();
            //获取当前处理时间
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            RecordGets.WillStateValue willStateValue = new RecordGets.WillStateValue(commitTime,filePaths);
            if (commitOp.equals("AppendCommit") || commitOp.equals("MergeCommit") || commitOp.equals("UpdateCommit")){
                if (compactState.contains(tableId + "/" + partitionDesc)){
                    if (commitTime <= currentProcessingTime - expiredTime){
                        //TODO
                        cleanUtils.write("delete this record:" + commitId+"/"+tableId+"/"+partitionDesc + filePaths);
                        cleanUtils.deleteFile(filePaths);
                        cleanUtils.deleteDataCommitInfo(tableId,commitId,partitionDesc);
                        cleanUtils.deletePartitionInfo(tableId,partitionDesc,commitId);
                    } else {
                        long triggerTime = currentProcessingTime + expiredTime;
                        willState.put(tableId + "/" + partitionDesc+"/"+commitId, willStateValue);
                        ctx.timerService().registerProcessingTimeTimer(triggerTime);
                    }
                } else {
                    willState.put(tableId + "/" +partitionDesc+"/"+commitId,willStateValue);
                }
            }
            //TODO 需要检查逻辑
            if (commitOp.equals("CompactionCommit")) {
                if (compactState.contains(tableId + "/" + partitionDesc)) {
                    if (commitTime > compactState.get(tableId + "/" + partitionDesc)) {
                        compactState.put(tableId + "/" + partitionDesc,commitTime);
                        willState.put(tableId + "/" + partitionDesc + "/" + commitId,willStateValue);
                    } else {
                        if (compactState.get(tableId + "/" + partitionDesc) < currentProcessingTime - expiredTime ){
                            //cleanUtils.write("delete this compact record:" + commitId+"/"+tableId+"/"+partitionDesc + filePaths);
                            cleanUtils.deleteFile(filePaths);
                            cleanUtils.deleteDataCommitInfo(tableId,commitId,partitionDesc);
                            cleanUtils.deletePartitionInfo(tableId,partitionDesc,commitId);
                        } else {
                            willState.put(tableId + "/" +partitionDesc+"/"+commitId,willStateValue);
                            long triggerTime = currentProcessingTime + expiredTime;
                            ctx.timerService().registerProcessingTimeTimer(triggerTime);
                        }
                    }
                } else {
                    compactState.put(tableId + "/" +partitionDesc, commitTime);
                    willState.put(tableId +"/" + partitionDesc + "/" + commitId,willStateValue);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            CleanUtils cleanUtils = new CleanUtils();
            Iterator iterator = compactState.iterator();
            while (iterator.hasNext()){
                Map.Entry<String,Long> entry = (Map.Entry<String,Long>) iterator.next();
                String compactId = entry.getKey();
                Long compactTime = entry.getValue();
                Iterator<Map.Entry<String, RecordGets.WillStateValue>> willStateIterator = willState.iterator();
                while (willStateIterator.hasNext()) {
                    Map.Entry<String, RecordGets.WillStateValue> willStateValueEntry = willStateIterator.next();
                    String willStateKey = willStateValueEntry.getKey();
                    RecordGets.WillStateValue willStateValueEntryValue = willStateValueEntry.getValue();
                    String tableId = willStateKey.split("/")[0];
                    String partitionDesc = willStateKey.split("/")[1];
                    String commitId = willStateKey.split("/")[2];
                    List<String> filePathList = willStateValueEntryValue.filePathList;
                    Long commitTime = willStateValueEntryValue.timestamp;
                    if ((tableId+"/"+partitionDesc).equals(compactId)){
                        //TODO 确定过期时间
                        if (commitTime < compactTime -  expiredTime ){
                            cleanUtils.deleteFile(filePathList);
                            cleanUtils.deleteDataCommitInfo(tableId,commitId,partitionDesc);
                            cleanUtils.deletePartitionInfo(tableId,partitionDesc,commitId);
                            willStateIterator.remove();
                        }
                    }
                }
            }
        }
    }
}
