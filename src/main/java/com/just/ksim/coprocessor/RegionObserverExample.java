package com.just.ksim.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-01-30 18:41
 * @modified by :
 **/
public class RegionObserverExample extends BaseRegionObserver {
    //TODO 直接修改Result类，增加一个添加cell的方法
    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s, final List<Result> results, int limit, boolean hasMore) throws IOException {
        List<Result> result1 = new ArrayList<>(results.size());
        for (Result result : results) {
            Put put = new Put(result.getRow());
            put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("test"), // co RegionObserverExample-2-Cell Create cell indirectly using a Put instance.
                    Bytes.toBytes(System.currentTimeMillis()));
            CellScanner scanner = put.cellScanner();
            scanner.advance();
            Cell cell = scanner.current();
            List<Cell> cells = result.listCells();
            cells.add(cell);
            result1.add(Result.create(cells));
        }
        results.clear();
        results.addAll(result1);
        return super.postScannerNext(e, s, results, limit, hasMore);
    }

//    @Override
//    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
//        return super.postScannerNext(e, s, results, limit, hasMore);
//    }
}
