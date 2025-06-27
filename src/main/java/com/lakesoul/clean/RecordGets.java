package com.lakesoul.clean;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class RecordGets {
    //private static final Logger logger = LoggerFactory.getLogger(PartitionLevelAssets.class);

    public static class metaMapper implements MapFunction<String, DataCommitInfo> {

        @Override
        public DataCommitInfo map(String s) {
            // 解析传入的 JSON 字符串
            JSONObject parse = (JSONObject) JSONObject.parse(s);
            String PGtableName = parse.get("tableName").toString();
            JSONObject commitJson;
            // 处理 data_commit_info 表
            if (PGtableName.equals("data_commit_info")) {
                String beforeCommitted = "true";
                if (parse.getJSONObject("before").size()>0){
                    JSONObject before =(JSONObject) parse.get("before");
                    beforeCommitted = before.getString("committed");
                }
                if (parse.getJSONObject("after").size() > 0){
                    commitJson = (JSONObject) parse.get("after");
                } else {
                    commitJson = (JSONObject) parse.get("before");
                }

                String eventOp = parse.getString("commitOp");
                if (!eventOp.equals("delete")) {
                    String tableId = commitJson.getString("table_id");
                    JSONArray fileOps = commitJson.getJSONArray("file_ops");
                    String committed = commitJson.getString("committed");
                    String commitOp = commitJson.getString("commit_op");
                    String commitId = commitJson.getString("commit_id");
                    long commitTime = commitJson.getLong("timestamp");
                    String partitionDesc = commitJson.getString("partition_desc");
                    if (fileOps != null && committed.equals("true")){
                        List<String> filePathList = new ArrayList<>();
                        for (Object op : fileOps) {
                            try {
                                byte[] decode = Base64.getDecoder().decode(op.toString());
                                String fileOp = new String(decode, StandardCharsets.UTF_8);
                                CleanUtils cleanUtils = new CleanUtils();
                                String[] fileOpsString = cleanUtils.parseFileOpsString(fileOp);
                                String filePath = fileOpsString[0];
                                filePathList.add(filePath);
                            } catch (NumberFormatException e) {
//                            logger.error("NumberFormatException :" + fileOps,e);
//                            logger.error("fileOp :" + new String(Base64.getDecoder().decode(op.toString()),StandardCharsets.UTF_8) );
//                            logger.error("decode :" + Arrays.toString(Base64.getDecoder().decode(op.toString())));
//                            logger.error("record: " + commitJson.toJSONString());
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        return new DataCommitInfo(tableId,commitTime,commitOp,commitId,partitionDesc,filePathList);
                    }
                } else {
                    List<String> list = new ArrayList<>();
                    return new DataCommitInfo("0",0l,"ss","ss","-5",list);
                }
            }
            List<String> list = new ArrayList<>();
            return new DataCommitInfo("0",0l,"ss","ss","-5",list);
        }
    }

    public static class DataCommitInfo {
        String table_id;
        String commit_id;
        String commit_op;
        long commit_time;
        String partition_desc;
        List<String> filePaths;

        public DataCommitInfo(String table_id, long commit_time, String commit_op, String commit_id, String partition_desc, List<String> filePaths) {
            this.table_id = table_id;
            this.commit_time = commit_time;
            this.commit_op = commit_op;
            this.commit_id = commit_id;
            this.partition_desc = partition_desc;
            this.filePaths = filePaths;
        }

        @Override
        public String toString() {
            return "DataCommitInfo{" +
                    "table_id='" + table_id + '\'' +
                    ", commit_id='" + commit_id + '\'' +
                    ", commit_op=" + commit_op +
                    ", commit_time=" + commit_time +
                    '}';
        }
    }

    public static class WillStateValue {
        Long timestamp;
        List<String> filePathList;

        public WillStateValue(Long timestamp, List<String> filePathList) {
            this.timestamp = timestamp;
            this.filePathList = filePathList;
        }
    }
}
