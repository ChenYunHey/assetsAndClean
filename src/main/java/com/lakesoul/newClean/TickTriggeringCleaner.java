package com.lakesoul.newClean;

import com.lakesoul.clean.CleanUtils;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.lakesoul.newClean.PartitionInfoRecordGets.PartitionInfo;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TickTriggeringCleaner extends RichCoFlatMapFunction<String, PartitionInfo, PartitionInfo> {

    private transient CleanUtils cleanUtils;
    private String pgUrl;
    private String userName;
    private String passWord;

    public TickTriggeringCleaner(String pgUrl, String userName, String passWord) {
        this.pgUrl = pgUrl;
        this.userName = userName;
        this.passWord = passWord;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        cleanUtils = new CleanUtils();
    }

    @Override
    public void flatMap1(String tick, Collector<PartitionInfo> out) throws Exception {
        try (Connection connection = DriverManager.getConnection(pgUrl, userName, passWord)) {
            cleanUtils.cleanDiscardFile(60000L, connection);

        } catch (SQLException e) {
            System.err.println("Failed to connect to database: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void flatMap2(PartitionInfo value, Collector<PartitionInfo> out) throws Exception {
        out.collect(value);
    }
}
