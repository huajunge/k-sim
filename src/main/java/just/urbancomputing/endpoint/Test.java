package just.urbancomputing.endpoint;

import coprocessor.generated.Sum;
import just.urbancomputing.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;

import java.io.IOException;
import java.util.Map;

import static just.urbancomputing.utils.Constants.DEFAULT_CF;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-09-14 19:14
 * @modified by :
 **/
public class Test {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("tdrive_tmp");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
//        String path = "file:///D:/projects/TraSS/target/TraSS-1.0-SNAPSHOT-jar-with-dependencies.jar";
//        hTableDescriptor.addFamily(new HColumnDescriptor(DEFAULT_CF));
//        hTableDescriptor.setValue("COPROCESSOR$knn", path + "|" + just.urbancomputing.endpoint.SumEndPoint.class.getCanonicalName() + "|" + Coprocessor.PRIORITY_USER);
//        Admin admin = connection.getAdmin();
//        admin.modifyTable(tableName, hTableDescriptor);
        Thread.sleep(200);
        Table table = connection.getTable(tableName);
        final Sum.SumRequest request = Sum.SumRequest.newBuilder().setFamily(Constants.DEFAULT_CF).setColumn(Constants.T_ID).build();
        try {
            Map<byte[], Long> results = table.coprocessorService(
                    Sum.SumService.class,
                    null,  /* start key */
                    null,  /* end   key */
                    new Batch.Call<Sum.SumService, Long>() {
                        @Override
                        public Long call(Sum.SumService aggregate) throws IOException {
                            CoprocessorRpcUtils.BlockingRpcCallback<Sum.SumResponse> rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<>();
                            aggregate.getSum(null, request, rpcCallback);
                            Sum.SumResponse response = rpcCallback.get();

                            return response.hasSum() ? response.getSum() : 0L;
                        }
                    }
            );

            for (Long sum : results.values()) {
                System.out.println("Sum = " + sum);
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
