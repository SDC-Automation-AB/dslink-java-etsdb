package org.dsa.iot.etsdb.db;

import org.dsa.iot.commons.ParameterizedAction;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.NodeUtils;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.handler.CompleteHandler;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.etsdb.serializer.ByteData;
import org.dsa.iot.etsdb.serializer.ValueSerializer;
import org.dsa.iot.historian.database.Database;
import org.dsa.iot.historian.utils.QueryData;
import org.etsdb.DatabaseFactory;
import org.etsdb.QueryCallback;
import org.etsdb.impl.DatabaseImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Samuel Grenier
 */
public class Db extends Database {

    private static final Logger LOGGER = LoggerFactory.getLogger(Db.class);
    private final DbProvider provider;
    private final String path;
    private final File fPath;

    private DatabaseImpl<ByteData> db;
    private boolean purgeable;
    private long diskSpaceRemaining;
    private ScheduledFuture<?> diskUsedMonitor;
    private ScheduledFuture<?> diskFreeMonitor;
    private ScheduledFuture<?> wpsMonitor;

    public Db(String name, String path, DbProvider provider) {
        super(name, provider);
        this.provider = provider;
        this.path = path;
        this.fPath = new File(path);
    }

    public DatabaseImpl<ByteData> getDb() {
        return db;
    }

    public File getPath() {
        return fPath;
    }

    public boolean isPurgeable() {
        return purgeable;
    }

    public long getDiskSpaceRemaining() {
        return diskSpaceRemaining;
    }

    private void setDiskSpaceRemaining(int space) {
        long totalSize = fPath.getTotalSpace();
        this.diskSpaceRemaining = (long) (totalSize * (space / 100.0));
    }

    @Override
    public void write(String path, Value value, long ts) {
        ByteData d = new ByteData();
        d.setValue(value);
        db.write(path, ts, d);
    }

    @Override
    public void query(String path,
                      long from,
                      long to,
                      final CompleteHandler<QueryData> handler) {
        if (db == null) {
            handler.complete();
            return;
        }
        
        db.query(path, from, to, new QueryCallback<ByteData>() {
            @Override
            public void sample(String seriesId, long ts, ByteData data) {
                handler.handle(data);
            }
        });
        handler.complete();
    }

    @Override
    public QueryData queryFirst(String path) {
        final QueryData data = new QueryData();
        
        if (db == null) {
            return null;
        }
        
        db.query(path, Long.MIN_VALUE,
                Long.MAX_VALUE, 1, new QueryCallback<ByteData>() {
                    @Override
                    public void sample(String seriesId, long ts, ByteData b) {
                        data.setTimestamp(ts);
                        data.setValue(b.getValue());
                    }
                });
        return data;
    }

    @Override
    public QueryData queryLast(String path) {
        final QueryData data = new QueryData();
        
        if (db == null) {
            return null;
        }
        
        db.query(path, Long.MIN_VALUE,
                Long.MAX_VALUE, 1, true, new QueryCallback<ByteData>() {
                    @Override
                    public void sample(String seriesId, long ts, ByteData b) {
                        data.setTimestamp(ts);
                        data.setValue(b.getValue());
                    }
                });
        return data;
    }

    @Override
    protected void performConnect() throws Exception {
        File d = new File(path);
        ValueSerializer vs = new ValueSerializer();
        db = DatabaseFactory.createDatabase(d, vs);
        provider.getPurger().addDb(this);
    }

    @Override
    public void close() throws Exception {
        provider.getPurger().removeDb(this);
        try {
            db.close();
        } finally {
            diskUsedMonitor.cancel(true);
            diskFreeMonitor.cancel(true);
            wpsMonitor.cancel(true);
        }
    }

    @Override
    public void initExtensions(final Node parent) {
        {
            NodeBuilder b = parent.createChild("edit");
            b.setDisplayName("Edit");
            {
                final Parameter nameParam;
                {
                    nameParam = new Parameter("Name", ValueType.STRING);
                    Value def = new Value(parent.getDisplayName());
                    nameParam.setDefaultValue(def);
                    nameParam.setDescription("Display name of the database");
                }

                final Parameter pathParam;
                {
                    pathParam = new Parameter("Path", ValueType.STRING);
                    Value def = parent.getConfig("path");
                    pathParam.setDefaultValue(def);
                    {
                        String desc = "Location of the database on the file ";
                        desc += "system. The path can absolute or relative. ";
                        desc += "Changing the path will cause the current ";
                        desc += "database to be moved to the new location.";
                        pathParam.setDescription(desc);
                    }
                }

                EditSettingsHandler a = new EditSettingsHandler();
                a.addParameter(nameParam);
                a.addParameter(pathParam);
                b.setAction(a);
            }
            b.build();
        }

        {
            NodeBuilder b = parent.createChild("purgeSettings");
            b.setDisplayName("Purge Settings");
            b.setRoConfig("ap", new Value(true));
            b.setRoConfig("dsr", new Value(10));
            {
                final Parameter purgeParam;
                {
                    purgeParam = new Parameter("Auto Purge", ValueType.BOOL);
                    Value def = NodeUtils.getRoConfig(b, "ap");
                    b.setRoConfig("ap", def);
                    purgeParam.setDefaultValue(def);
                    {
                        String desc = "Whether the database is allowed to ";
                        desc += "be purged automatically.";
                        purgeParam.setDescription(desc);
                    }
                }

                final Parameter spaceParam;
                {
                    spaceParam = new Parameter("Disk Space Remaining", ValueType.NUMBER);
                    Value def = NodeUtils.getRoConfig(b, "dsr");
                    b.setRoConfig("dsr", def);
                    spaceParam.setDefaultValue(def);
                    {
                        String desc = "Controls how much disk space should be ";
                        desc += "remaining before a purge gets ran based on ";
                        desc += "a percentage of the total disk space on the ";
                        desc += "partition that the database is stored on";
                        spaceParam.setDescription(desc);
                    }
                }

                PurgeSettingsHandler a = new PurgeSettingsHandler();
                a.addParameter(purgeParam);
                a.addParameter(spaceParam);
                b.setAction(a);
            }
            Node node = b.build();
            purgeable = node.getRoConfig("ap").getBool();
            Number n = node.getRoConfig("dsr").getNumber();
            setDiskSpaceRemaining(n.intValue());
        }

        {
            NodeBuilder b = parent.createChild("dap");
            b.setDisplayName("Delete and Purge");
            b.setSerializable(false);
            b.setAction(new Action(getProvider().dbPermission(),
                    new Handler<ActionResult>() {
                        @Override
                        public void handle(ActionResult event) {
                            getProvider().deleteDb(parent);
                            deleteDirectory(fPath);
                        }
                    }));
            b.build();
        }

        {
            NodeBuilder b = parent.createChild("wps");
            b.setDisplayName("Writes Per Second");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getWritesPerSecond()));
            b.setSerializable(false);
            final Node node = b.build();

            ScheduledThreadPoolExecutor stpe = Objects.getDaemonThreadPool();
            wpsMonitor = stpe.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    int wps = db.getWritesPerSecond();
                    Value prev = node.getValue();
                    if (prev != null && prev.getNumber().intValue() != wps) {
                        node.setValue(new Value(wps));
                    }
                }
            }, 1, 1, TimeUnit.SECONDS);
        }

        {
            NodeBuilder b = parent.createChild("rw");
            b.setDisplayName("Rows Written");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getWriteCount()));
            final Node node = b.build();
            db.setWriteCount(node.getValue().getNumber().longValue());
            db.setWriteCountHandler(new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    Value prev = node.getValue();
                    if (prev != null && prev.getNumber().longValue() != event) {
                        node.setValue(new Value(event));
                    }
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("rf");
            b.setDisplayName("Rows Flushed");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getFlushCount()));
            final Node node = b.build();
            db.setFlushCount(node.getValue().getNumber().longValue());
            db.setFlushCountHandler(new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("rc");
            b.setDisplayName("Rows Cached");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getQueueSize()));
            final Node node = b.build();
            node.setSerializable(false);
            db.setQueueSizeHandler(new Handler<Integer>() {
                @Override
                public void handle(Integer event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("fcf");
            b.setDisplayName("Forced Rows Flushed");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getFlushForced()));
            final Node node = b.build();
            db.setFlushForced(node.getValue().getNumber().longValue());
            db.setFlushForcedHandler(new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("cef");
            b.setDisplayName("Cache Expiry Flushes");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getFlushExpired()));
            final Node node = b.build();
            db.setFlushExpired(node.getValue().getNumber().longValue());
            db.setFlushExpiredHandler(new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("clf");
            b.setDisplayName("Cache Limit Flushes");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getFlushLimit()));
            final Node node = b.build();
            db.setFlushLimit(node.getValue().getNumber().longValue());
            db.setFlushLimitHandler(new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("lfd");
            b.setDisplayName("Last Flush Duration");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getLastFlushMillis()));
            final Node node = b.build();
            node.setSerializable(false);
            db.setLastFlushMillisHandler(new Handler<Integer>() {
                @Override
                public void handle(Integer event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("brw");
            b.setDisplayName("Backdated Rows written");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getBackdateCount()));
            final Node node = b.build();
            db.setBackdateCount(node.getValue().getNumber().longValue());
            db.setBackdateCountHandler(new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("os");
            b.setDisplayName("Open Shards");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getOpenShards()));
            final Node node = b.build();
            node.setSerializable(false);
            db.setOpenShardsHandler(new Handler<Integer>() {
                @Override
                public void handle(Integer event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("of");
            b.setDisplayName("Open Files");
            b.setValueType(ValueType.NUMBER);
            b.setValue(new Value(db.getOpenFiles()));
            final Node node = b.build();
            node.setSerializable(false);
            db.setOpenFilesHandler(new Handler<Integer>() {
                @Override
                public void handle(Integer event) {
                    node.setValue(new Value(event));
                }
            });
        }

        {
            NodeBuilder b = parent.createChild("dbs");
            b.setDisplayName("Database Size");
            b.setSerializable(false);
            b.setValueType(ValueType.NUMBER);
            b.setConfig("unit", new Value("MiB"));
            final Node node;
            {
                Node tmp = b.getParent().getChild("dbs", false);
                if (tmp == null) {
                    node = b.getChild();
                } else {
                    node = tmp;
                }
            }

            ScheduledThreadPoolExecutor stpe = Objects.getDaemonThreadPool();
            diskUsedMonitor = stpe.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    double size = db.getDatabaseSize();
                    size /= 1048576;
                    Value prev = node.getValue();
                    if (prev == null
                            || Math.abs(size - prev.getNumber().doubleValue()) > .0001) {
                        node.setValue(new Value(size));
                    }

                }
            }, 0, 5, TimeUnit.SECONDS);
            b.build();
        }
        {
            NodeBuilder b = parent.createChild("sa");
            b.setDisplayName("Space Available");
            b.setSerializable(false);
            b.setValueType(ValueType.NUMBER);
            b.setConfig("unit", new Value("MiB"));
            final Node node;
            {
                Node tmp = b.getParent().getChild("sa", false);
                if (tmp == null) {
                    node = b.getChild();
                } else {
                    node = tmp;
                }
            }

            ScheduledThreadPoolExecutor stpe = Objects.getDaemonThreadPool();
            diskFreeMonitor = stpe.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    double size = db.availableSpace();
                    size /= 1048576;
                    Value prev = node.getValue();
                    if (prev == null
                            || Math.abs(size - prev.getNumber().doubleValue()) > .0001) {
                        node.setValue(new Value(size));
                    }

                }
            }, 0, 5, TimeUnit.SECONDS);
            b.build();
        }
    }

    private static void deleteDirectory(File path) {
        File[] files = path.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isFile()) {
                    if (!f.delete()) {
                        LOGGER.error("Failed to delete: {}", f.getAbsolutePath());
                    }
                } else {
                    deleteDirectory(f);
                }
            }
        }
        if (!path.delete()) {
            LOGGER.error("Failed to delete: {}", path.getAbsolutePath());
        }
    }

    private class EditSettingsHandler extends ParameterizedAction {

        public EditSettingsHandler() {
            super(getProvider().dbPermission());
        }

        @Override
        public void handle(ActionResult event, Map<String, Value> params) {
            Node node = event.getNode().getParent();

            Value vName = params.get("Name");
            node.setDisplayName(vName.getString());

            Value vPath = params.get("Path");
            String path = node.getConfig("path").getString();
            if (!path.equals(vPath.getString())) {
                node.setConfig("path", vPath);
                db.move(new File(vPath.getString()));
            }
        }
    }

    private class PurgeSettingsHandler extends ParameterizedAction {

        public PurgeSettingsHandler() {
            super(getProvider().dbPermission());
        }

        @Override
        public void handle(ActionResult event, Map<String, Value> params) {
            Node node = event.getNode();

            Value vP = params.get("Auto Purge");
            node.setRoConfig("ap", vP);

            Value vD = params.get("Disk Space Remaining");
            if (vD.getNumber().intValue() < 0) {
                vD.set(0);
            } else if (vD.getNumber().intValue() > 100) {
                vD.set(100);
            }
            node.setRoConfig("dsr", vD);

            purgeable = vP.getBool();
            setDiskSpaceRemaining(vD.getNumber().intValue());
        }
    }
}
