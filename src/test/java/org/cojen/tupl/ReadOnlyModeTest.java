package org.cojen.tupl;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.cojen.tupl.TestUtils.fastAssertArrayEquals;
import static org.cojen.tupl.TestUtils.newTempDatabase;
import static org.cojen.tupl.TestUtils.reopenTempDatabase;
import static org.cojen.tupl.TestUtils.sleep;

/**
 * Created by tylaar on 15/8/2.
 */
public class ReadOnlyModeTest {
    final static int rateMillis = 500;

    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SnapshotTest.class.getName());
    }

    protected void decorate(DatabaseConfig config) throws Exception {
    }


    public void disableCheckpointCreation() throws Exception {

        Database dbRW = createTempRWDatabaseForROTest();

        writeTestDataInRWDataBase(dbRW);

        DatabaseConfig configRO = new DatabaseConfig()
                .checkpointRate(rateMillis, TimeUnit.MILLISECONDS)
                .checkpointSizeThreshold(0)
                .checkpointDelayThreshold(0, null)
                .readOnly(true)
                .durabilityMode(DurabilityMode.NO_FLUSH);
        decorate(configRO);
        Database dbRO = reopenTempDatabase(dbRW, configRO);
        Index ixRO = dbRO.openIndex("readOnly");
        fastAssertArrayEquals("world".getBytes(), ixRO.load(null, "hello".getBytes()));
        dbRO.checkpoint();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void disableCheckpointSuspend() throws Exception {

        Database dbRW = createTempRWDatabaseForROTest();

        writeTestDataInRWDataBase(dbRW);

        DatabaseConfig configRO = new DatabaseConfig()
                .checkpointRate(rateMillis, TimeUnit.MILLISECONDS)
                .checkpointSizeThreshold(0)
                .checkpointDelayThreshold(0, null)
                .readOnly(true)
                .durabilityMode(DurabilityMode.NO_FLUSH);
        decorate(configRO);
        Database dbRO = reopenTempDatabase(dbRW, configRO);
        Index ixRO = dbRO.openIndex("readOnly");
        fastAssertArrayEquals("world".getBytes(), ixRO.load(null, "hello".getBytes()));
        dbRO.suspendCheckpoints();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void disableCheckpointResume() throws Exception {

        Database dbRW = createTempRWDatabaseForROTest();

        writeTestDataInRWDataBase(dbRW);

        DatabaseConfig configRO = new DatabaseConfig()
                .checkpointRate(rateMillis, TimeUnit.MILLISECONDS)
                .checkpointSizeThreshold(0)
                .checkpointDelayThreshold(0, null)
                .readOnly(true)
                .durabilityMode(DurabilityMode.NO_FLUSH);
        decorate(configRO);
        Database dbRO = reopenTempDatabase(dbRW, configRO);
        Index ixRO = dbRO.openIndex("readOnly");

        fastAssertArrayEquals("world".getBytes(), ixRO.load(null, "hello".getBytes()));
        dbRO.resumeCheckpoints();
    }

    @Test
    public void openNotExistIndexInReadOnly() throws Exception {

        Database dbRW = createTempRWDatabaseForROTest();

        writeTestDataInRWDataBase(dbRW);

        DatabaseConfig configRO = new DatabaseConfig()
                .checkpointRate(rateMillis, TimeUnit.MILLISECONDS)
                .checkpointSizeThreshold(0)
                .checkpointDelayThreshold(0, null)
                .readOnly(true)
                .durabilityMode(DurabilityMode.NO_FLUSH);
        decorate(configRO);
        Database dbRO = reopenTempDatabase(dbRW, configRO);
        Index ixRO = dbRO.openIndex("readWrite");
        Assert.assertNull(ixRO);
    }


    private Database createTempRWDatabaseForROTest() throws Exception {
        final int rateMillis = 500;
        DatabaseConfig configRW = new DatabaseConfig()
                .checkpointRate(rateMillis, TimeUnit.MILLISECONDS)
                .checkpointSizeThreshold(0)
                .checkpointDelayThreshold(0, null)
                .durabilityMode(DurabilityMode.NO_FLUSH);
        decorate(configRW);
        return newTempDatabase(configRW);
    }

    private void writeTestDataInRWDataBase(final Database dbRW) throws IOException {
        Index ix = dbRW.openIndex("readOnly");
        ix.store(Transaction.BOGUS, "hello".getBytes(), "world".getBytes());
        sleep(rateMillis * 2);
        dbRW.close();
    }
}
