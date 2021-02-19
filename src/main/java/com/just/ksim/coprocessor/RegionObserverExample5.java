package com.just.ksim.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// cc RegionObserverExample Example region observer checking for special get requests
// vv RegionObserverExample
public class RegionObserverExample5 extends BaseRegionObserver {
    // ^^ RegionObserverExample
    public static final Log LOG = LogFactory.getLog(HRegion.class);
    // vv RegionObserverExample
    public static final byte[] FIXED_ROW = Bytes.toBytes("@@@GETTIME@@@");

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e,
                         Get get, List<Cell> results) throws IOException {
        // ^^ RegionObserverExample
        LOG.debug("Got preGet for row: " + Bytes.toStringBinary(get.getRow()));
        // vv RegionObserverExample
        if (Bytes.equals(get.getRow(), FIXED_ROW)) { // co RegionObserverExample-1-Check Check if the request row key matches a well known one.
            Put put = new Put(get.getRow());
            put.addColumn(FIXED_ROW, FIXED_ROW, // co RegionObserverExample-2-Cell Create cell indirectly using a Put instance.
                    Bytes.toBytes(System.currentTimeMillis()));
            CellScanner scanner = put.cellScanner();
            scanner.advance();
            Cell cell = scanner.current(); // co RegionObserverExample-3-Current Get first cell from Put using the CellScanner instance.
            // ^^ RegionObserverExample
            LOG.debug("Had a match, adding fake cell: " + cell);
            // vv RegionObserverExample
            results.add(cell); // co RegionObserverExample-4-Create Create a special KeyValue instance containing just the current time on the server.
        }
    }

    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
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
            //result1.add(Result.create(result.listCells()));
        }
        //results.clear();
        results.addAll(result1);
        return hasMore;
    }
}
// ^^ RegionObserverExample
