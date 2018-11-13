package org.dsa.iot.etsdb.db;

import org.dsa.iot.dslink.DSLinkHandler;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.EditorType;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.StringUtils;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.etsdb.serializer.ByteData;
import org.dsa.iot.historian.database.Database;
import org.dsa.iot.historian.database.DatabaseProvider;
import org.dsa.iot.historian.database.Watch;
import org.dsa.iot.historian.utils.TimeParser;
import org.etsdb.TypeOverrideTypes;
import org.etsdb.impl.DatabaseImpl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Samuel Grenier
 */
public class DbProvider extends DatabaseProvider {

    private final DbPurger purger = new DbPurger();

    public DbProvider() {
        purger.setupPurger();
    }

    public DbPurger getPurger() {
        return purger;
    }

    public void stop() {
        purger.stop();
    }

    @Override
    public Action createDbAction(Permission perm) {
        Action act = new Action(perm,
                new Handler<ActionResult>() {
                    @Override
                    public void handle(ActionResult event) {
                        Value vPath = event.getParameter("Path", ValueType.STRING);
                        String path = vPath.getString();

                        String name = StringUtils.encodeName(path);
                        NodeBuilder builder = createDbNode(name, event);
                        builder.setConfig("path", new Value(path));

                        Value vName = event.getParameter("Name");
                        if (vName != null) {
                            builder.setDisplayName(vName.getString());
                        } else {
                            builder.setDisplayName(path);
                        }

                        createAndInitDb(builder.build());
                    }
                });
        {
            Parameter p = new Parameter("Name", ValueType.STRING);
            String desc = "Name of the database\n";
            desc += "If name is not provided, the path will be used for the name";
            p.setDescription(desc);
            act.addParameter(p);

            Value def = new Value("db");
            p = new Parameter("Path", ValueType.STRING, def);
            desc = "Absolute or relative path to the database\n";
            desc += "If the database does not exist, it will be created";
            p.setDescription(desc);
            act.addParameter(p);
        }
        return act;
    }

    @Override
    public Database createDb(Node node) {
        String path = node.getConfig("path").getString();
        DSLinkHandler h = node.getLink().getHandler();
        Path p = Paths.get(h.getWorkingDir().getAbsolutePath());
        path = p.resolve(path).toString();
        return new Db(node.getDisplayName(), path, this);
    }

    @Override
    public Permission dbPermission() {
        return Permission.CONFIG;
    }

    @Override
    public void onWatchAdded(final Watch watch) {
        final Node node = watch.getNode();
        final Database database = watch.getGroup().getDb();
        final Permission perm = database.getProvider().dbPermission();
        {
            NodeBuilder b = node.createChild("unsubPurge");
            b.setDisplayName("Unsubscribe and Purge");
            Action a = new Action(perm, new Handler<ActionResult>() {
                @Override
                public void handle(ActionResult event) {
                    watch.unsubscribe();

                    String path = node.getName();
                    Value useNewEncodingMethod = watch.getNode().getConfig(Watch.USE_NEW_ENCODING_METHOD_CONFIG_NAME);
                    if (useNewEncodingMethod == null || !useNewEncodingMethod.getBool()) {
                        path = path.replaceAll("%2F", "/").replaceAll("%2E", ".");
                    } else {
                        path = watch.getPath();
                    }
                    DatabaseImpl<ByteData> db = ((Db) database).getDb();
                    db.deleteSeries(path);
                }
            });
            b.setAction(a);
            b.build();
        }
        {
            NodeBuilder b = node.createChild("purge");
            b.setDisplayName("Purge");
            Action a = new Action(perm, new Handler<ActionResult>() {
                @Override
                public void handle(ActionResult event) {
                    long fromTs = Long.MIN_VALUE;
                    long toTs = Long.MAX_VALUE;

                    Value vTR = event.getParameter("Timerange");
                    if (vTR != null) {
                        String[] split = vTR.getString().split("/");
                        fromTs = TimeParser.parse(split[0]);
                        toTs = TimeParser.parse(split[1]);
                    }

                    String path = node.getName();
                    path = StringUtils.decodeName(path);

                    DatabaseImpl<ByteData> db = ((Db) database).getDb();
                    db.delete(path, fromTs, toTs);
                }
            });
            {
                Parameter p = new Parameter("Timerange", ValueType.STRING);
                {
                    String desc = "The range for which to purge data";
                    p.setDescription(desc);
                }
                p.setEditorType(EditorType.DATE_RANGE);
                a.addParameter(p);
            }
            b.setAction(a);
            b.build();
        }

        addOverrideTypeAction(node, perm);
        final Value overrideTypeVal = node.getRoConfig("overrideType");
        if (overrideTypeVal != null) {
        	node.setValueType(ValueType.toValueType(overrideTypeVal.getString()));
        	Objects.getDaemonThreadPool().schedule(new Runnable() {
    			@Override
    			public void run() {
    				node.setValueType(ValueType.toValueType(overrideTypeVal.getString()));
    			}
            }, 500, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void deleteRange(Watch watch, long fromTs, long toTs) {
        final Database database = watch.getGroup().getDb();
        DatabaseImpl<ByteData> db = ((Db) database).getDb();
        db.delete(watch.getPath(), fromTs, toTs);
    }

    private void addOverrideTypeAction(final Node node, Permission permission) {
        NodeBuilder nodeBuilder = node.createChild("overrideType");
        nodeBuilder.setDisplayName("Override data point type");

        Action action = new Action(permission, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                Value overrideType = event.getParameter("TypeName");

                if (overrideType == null) {
                	node.removeRoConfig("overrideType");
                    return;
                }

                String typeAsString = overrideType.getString();

                if (TypeOverrideTypes.NONE == TypeOverrideTypes.fromName(typeAsString)) {
                	node.removeRoConfig("overrideType");
                    return;
                }

                node.setRoConfig("overrideType", new Value(typeAsString));
                node.setValueType(ValueType.toValueType(typeAsString));
            }
        });

        Set<String> types = TypeOverrideTypes.buildEnums();
        Parameter overrideToTypeParameter = new Parameter("TypeName", ValueType.makeEnum(types));
        overrideToTypeParameter.setDescription("The type the data point type will be overridden to. " +
                "If None is selected, no override will be done");
        action.addParameter(overrideToTypeParameter);

        nodeBuilder.setAction(action);
        nodeBuilder.build();
    }
}
