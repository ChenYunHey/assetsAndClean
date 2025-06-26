package lakesoul.clean;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class GetUsefulRecord extends ProcessFunction<RecordGets.DataCommitInfo, RecordGets.DataCommitInfo> {
    @Override
    public void processElement(RecordGets.DataCommitInfo dataCommitInfo, Context context, Collector<RecordGets.DataCommitInfo> collector) throws Exception {
        String tableID = dataCommitInfo.table_id;
        if (!tableID.equals("0")) {
            collector.collect(dataCommitInfo);
        }
    }
}
