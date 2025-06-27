package com.lakesoul.newClean;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

public class PartitionInfoRecordGets {

    public static class metaMapper implements MapFunction<String, PartitionInfo>{

        @Override
        public PartitionInfo map(String value) throws Exception {
            JSONObject parse = (JSONObject) JSONObject.parse(value);
            String PGtableName = parse.get("tableName").toString();
            JSONObject commitJson = null;
            PartitionInfo partitionInfo = null;
            if (PGtableName.equals("partition_info")){
                if (!parse.getJSONObject("after").isEmpty()){
                    commitJson = (JSONObject) parse.get("after");
                } else {
                    commitJson = (JSONObject) parse.get("before");
                }
            }
            String eventOp = parse.getString("commitOp");
            if (!eventOp.equals("delete")){
                String table_id = commitJson.getString("table_id");
                String partition_desc = commitJson.getString("partition_desc");
                String commit_op = commitJson.getString("commit_op");
                int version = commitJson.getInteger("version");
                long timestamp = commitJson.getLong("timestamp");
                JSONArray snapshot = commitJson.getJSONArray("snapshot");
                List<String> snapshotList = new ArrayList<>();
                for (int i = 0; i < snapshot.size(); i++) {
                    snapshotList.add(snapshot.get(i).toString());
                }
                partitionInfo = new PartitionInfo(table_id,partition_desc,version,commit_op,timestamp, snapshotList);
            }
            return partitionInfo;
        }
    }

    public static class PartitionInfo{

        String table_id;
        String partition_desc;
        int version;
        String commit_op;
        long timestamp;
        List<String> snapshot;

        public PartitionInfo(String table_id, String partition_dec, int version, String commit_op, long timestamp, List<String> snapshot) {
            this.table_id = table_id;
            this.partition_desc = partition_dec;
            this.version = version;
            this.commit_op = commit_op;
            this.timestamp = timestamp;
            this.snapshot = snapshot;
        }

        @Override
        public String toString() {
            return "PartitionInfo{" +
                    "table_id='" + table_id + '\'' +
                    ", partition_dec='" + partition_desc + '\'' +
                    ", version=" + version +
                    ", commit_op='" + commit_op + '\'' +
                    ", timestamp=" + timestamp +
                    ", snapshot=" + snapshot +
                    '}';
        }

        public static class WillStateValue {
            Long timestamp;
            List<String> snapshot;

            public WillStateValue(Long timestamp, List<String> snapshot) {
                this.timestamp = timestamp;
                this.snapshot = snapshot;
            }
        }
    }
}
