import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import static spark.Spark.*;


public class KafkaBroker {
    public static String KAFKA_BROKERS;
    public static String KAFKA_ZOOKEEPERS;

    static class Instance {
        public String KAFKA_BROKERS;
        public String KAFKA_TOPIC;
        public String KAFKA_ZOOKEEPERS;
    }

    static class InstancePayload {
        public String plan;
        public String billingcode;

        public boolean isValid() {
            return plan != null && billingcode != null;
        }
    }

    static class Tagspec {
        public String resource;
        public String name;
        public String value;

        public boolean isValid() {
            return resource != null && name != null && value != null;
        }
    }

    static class Planspec {
        public String small;
        public String medium;
        public String large;
    }

    static class ErrorMessagespec {
        public int status;
        public String message;

        public ErrorMessagespec(int status_, String message_) {
            this.status = status_;
            this.message = message_;
        }
    }

    static class OKMessagespec {
        public int status;
        public String message;

        public OKMessagespec(int status_, String message_) {
            this.status = status_;
            this.message = message_;
        }
    }

    static class TagList {
        public Object[] tags;

        public TagList(Object
                               [] tags_) {
            this.tags = tags_;
        }
    }
    public KafkaBroker() {

    }

    public static void main(String[] args) {
        KafkaBroker kb = new KafkaBroker();
        kb.runServer();
    }


    public void runServer() {

        KAFKA_BROKERS = System.getenv("KAFKA_BROKERS");
        KAFKA_ZOOKEEPERS = System.getenv("KAFKA_ZOOKEEPERS");
        Logger.getRootLogger().setLevel(Level.OFF);
        port(Integer.parseInt(System.getenv("PORT")));

        post("/v1/kafka/instance", (req, res) -> {
            ObjectMapper mapper = new ObjectMapper();
            InstancePayload instancepayload = mapper.readValue(req.body(), InstancePayload.class);
            if (!instancepayload.isValid()) {
                ErrorMessagespec ems = new ErrorMessagespec(400, "Bad Request");
                res.status(400);
                res.type("application/json");
                return new Gson().toJson(ems);
            }
            System.out.println(instancepayload.billingcode);
            System.out.println(instancepayload.plan);
            Object result = provisionTLHandler(instancepayload);
            if (result instanceof Instance) {
                res.status(201);
                res.type("application/json");
                return new Gson().toJson((Instance) result);
            } else if (result instanceof ErrorMessagespec) {
                res.status(((ErrorMessagespec) result).status);
                res.type("application/json");
                return new Gson().toJson(result);
            } else {
                res.status(500);
                res.type("application/json");
                return new Gson().toJson(new ErrorMessagespec(500, "Unknown Error"));
            }
        });


        get("/v1/kafka/url/:name", (req, res) -> {
            Object result = getUrlTLHandler(req.params(":name"));
            if (result instanceof Instance) {
                res.status(200);
                res.type("application/json");
                return new Gson().toJson(result);
            } else if (result instanceof ErrorMessagespec) {
                res.type("application/json");
                res.status(((ErrorMessagespec) result).status);
                return new Gson().toJson(result);
            } else {
                res.status(500);
                res.type("application/json");
                return new Gson().toJson(new ErrorMessagespec(500, "Unknown Error"));
            }
        });


        delete("/v1/kafka/instance/:topic", (req, res) -> {
            Object result = deleteTopicTLHandler(req.params(":topic"));
            if (result instanceof OKMessagespec) {
                res.status(200);
                res.type("application/json");
                return new Gson().toJson(result);
            } else if (result instanceof ErrorMessagespec) {
                res.status(((ErrorMessagespec) result).status);
                res.type("application/json");
                return new Gson().toJson(result);
            } else {
                res.status(500);
                res.type("application/json");
                return new Gson().toJson(new ErrorMessagespec(500, "Unknown Error"));
            }
        });


        get("/v1/kafka/plans", (req, res) -> {
            Planspec plans = new Planspec();
            plans.small = "1 partition, 1 replica, 1 hour ";
            plans.medium = "2 partitions, 2 replicas, 10 hours";
            plans.large = "6 partitions, 2 replicas, 5 days";
            res.status(200);
            res.type("application/json");
            return new Gson().toJson(plans);
        });


        post("/v1/kafka/tag", (req, res) -> {
            ObjectMapper mapper = new ObjectMapper();
            Tagspec tagobj = mapper.readValue(req.body(), Tagspec.class);
            if (!tagobj.isValid()) {
                ErrorMessagespec ems = new ErrorMessagespec(400, "Bad Request");
                res.status(400);
                res.type("application/json");
                return new Gson().toJson(ems);
            }
            System.out.println(tagobj.resource);
            System.out.println(tagobj.name);
            System.out.println(tagobj.value);
            Object result = addTagTLHandler(tagobj);
            if (result instanceof OKMessagespec) {
                res.status(200);
                res.type("application/json");
                return new Gson().toJson(result);
            } else if (result instanceof ErrorMessagespec) {
                res.status(((ErrorMessagespec) result).status);
                res.type("application/json");
                return new Gson().toJson(result);
            } else {
                res.status(500);
                res.type("application/json");
                return new Gson().toJson(new ErrorMessagespec(500, "Unknown Error"));
            }
        });
        get("/v1/kafka/tags/:topic", (req, res) -> {
            Object result = getTagsTLHandler(req.params(":topic"));
            if (result instanceof TagList) {
                res.status(200);
                res.type("application/json");
                return new Gson().toJson(result);
            } else if (result instanceof ErrorMessagespec) {
                res.status(((ErrorMessagespec) result).status);
                res.type("application/json");
                return new Gson().toJson(result);
            } else {
                res.status(500);
                res.type("application/json");
                return new Gson().toJson(new ErrorMessagespec(500, "Unknown Error"));
            }
        });
    }

    public Object provisionTLHandler(InstancePayload instance) {
        Object toreturn;

        String uuid = UUID.randomUUID().toString();
        String nameprefix = System.getenv("NAME_PREFIX");
        String name = nameprefix + uuid.split("-")[0];
        System.out.println(name);
        String partitions = null;
        String replicas = null;
        String timetokeep = null;
        if (instance.plan.equalsIgnoreCase("small")) {
            partitions = "1";
            replicas = "1";
            timetokeep = "3600000";     // 1 hour
        }
        if (instance.plan.equalsIgnoreCase("medium")) {
            partitions = "2";
            replicas = "2";
            timetokeep = "36000000";    //10 hours
        }
        if (instance.plan.equalsIgnoreCase("large")) {
            partitions = "6";
            replicas = "2";
            timetokeep = "432000000"; //5 days
        }

        try {
            createTopic(name, partitions, replicas, timetokeep);
        } catch (Exception e) {
            e.printStackTrace();
            ErrorMessagespec errormessage = new ErrorMessagespec(500, e.getMessage());
            toreturn = errormessage;
            return toreturn;
        }
        try {
            insertNew(name, instance.plan);
        } catch (Exception e) {
            e.printStackTrace();
            ErrorMessagespec errormessage = new ErrorMessagespec(500, e.getMessage());
            toreturn = errormessage;
            return toreturn;
        }

        try {
            Object result = getUrlTLHandler(name);
            toreturn = result;
        } catch (Exception e) {
            e.printStackTrace();
            ErrorMessagespec errormessage = new ErrorMessagespec(500, e.getMessage());
            toreturn = errormessage;
            return toreturn;

        }
        return toreturn;

    }

    private Object getUrlTLHandler(String name) {

        Instance instance = new Instance();
        instance.KAFKA_BROKERS = KAFKA_BROKERS;
        instance.KAFKA_TOPIC = name;
        instance.KAFKA_ZOOKEEPERS = KAFKA_ZOOKEEPERS;
        return instance;

    }

    private Connection connectToDatabaseOrDie() {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            String url = System.getenv("BROKERDB");
            conn = DriverManager.getConnection(url, System.getenv("BROKERDBUSER"), System.getenv("BROKERDBPASS"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }


    public void createTopic(String args, String partitions, String replicas, String timetokeep) throws Exception {
        ZkClient zkClient = null;
        ZkUtils zkUtils = null;
        Object toreturn;
        try {
            String zookeeperHosts = KAFKA_ZOOKEEPERS;
            int sessionTimeOutInMs = 15 * 1000;
            int connectionTimeOutInMs = 10 * 1000;

            zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);

            String topicName = args;
            int noOfPartitions = Integer.parseInt(partitions);
            ;
            int noOfReplication = Integer.parseInt(replicas);
            Properties topicConfiguration = new Properties();
            topicConfiguration.setProperty("retention.ms", timetokeep);

            AdminUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplication, topicConfiguration);

        } catch (Exception ex) {
            throw ex;
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }

    }

    public Object deleteTopicTLHandler(String args) {
        Object toreturn;
        ZkClient zkClient = null;
        ZkUtils zkUtils = null;
        try {
            String zookeeperHosts = KAFKA_ZOOKEEPERS;
            int sessionTimeOutInMs = 15 * 1000;
            int connectionTimeOutInMs = 10 * 1000;

            zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);

            String topicName = args;
            System.out.println("about to delete " + topicName);
            AdminUtils.deleteTopic(zkUtils, topicName);
            deleteRow(topicName);

        } catch (Exception ex) {
            toreturn = new ErrorMessagespec(500, ex.getMessage());
            return toreturn;
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
        return new OKMessagespec(200, "Topic deleted");
    }

    private void insertNew(String name, String plan) throws Exception {
        Connection conn = null;
        conn = connectToDatabaseOrDie();
        try {
            String getinstance = "insert into provision (name,plan,claimed) values (?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(getinstance);
            stmt.setString(1, name);
            stmt.setString(2, plan);
            stmt.setString(3, "yes");
            stmt.executeUpdate();

            stmt.close();
            conn.close();
        } catch (Exception e) {
            throw e;
        }

    }


    private void deleteRow(String name) throws Exception {
        Connection conn = null;
        conn = connectToDatabaseOrDie();
        try {
            String deleteinstance = "delete from  provision where name = ?";
            PreparedStatement stmt = conn.prepareStatement(deleteinstance);
            stmt.setString(1, name);
            int updated = stmt.executeUpdate();
            System.out.println(updated);
            stmt.close();
            conn.close();
        } catch (Exception e) {
            throw e;
        }
    }


    private Object addTagTLHandler(Tagspec tag) {
        Object toreturn;
        try {
            Object[] existingtags = getTags(tag.resource);
            Object[] newtags = appendValue(existingtags, tag);
            updateTagsDB(tag.resource, newtags);
        } catch (Exception e) {
            e.printStackTrace();
            return new ErrorMessagespec(500, "Error Adding Tag");
        }
        return new OKMessagespec(201, "Tag Added");
    }

    private Object getTagsTLHandler(String name) {
        Object toreturn;
        try {
            TagList tl = new TagList(getTags(name));
            toreturn = tl;
        } catch (Exception e) {

            e.printStackTrace();
            toreturn = new ErrorMessagespec(500, e.getMessage());
            return toreturn;
        }
        return toreturn;

    }
    private Object[] getTags(String name) throws Exception {
        Object[] tagsa = new Tagspec[0];
        Connection conn = null;
        conn = connectToDatabaseOrDie();
        try {
            String tagsquery = "select tags from provision where name=?";
            PreparedStatement stmt = conn.prepareStatement(tagsquery);
            stmt.setString(1, name);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String tagjson = rs.getString(1);
                if (tagjson != null) {
                    JsonArray jsonArray = new JsonParser().parse(tagjson).getAsJsonArray();
                    for (int i = 0; i < jsonArray.size(); i++) {
                        Gson gson = new Gson();
                        JsonElement str = jsonArray.get(i);
                        Tagspec obj = gson.fromJson(str, Tagspec.class);
                        System.out.println(obj);
                        System.out.println(str);
                        tagsa = appendValue(tagsa, obj);
                        System.out.println("-------");
                    }
                } else {
                }

            }
        } catch (Exception e) {
            throw e;
        }
        return tagsa;
    }

    private Object[] appendValue(Object[] obj, Tagspec newObj) {

        ArrayList<Object> temp = new ArrayList<Object>(Arrays.asList(obj));
        temp.add(newObj);
        return temp.toArray();

    }

    private void updateTagsDB(String name, Object[] tagsa) throws Exception {
        Connection conn = null;
        conn = connectToDatabaseOrDie();
        try {
            String getinstance = "update provision set tags = to_json(?::json) where name= ?";
            PreparedStatement stmt = conn.prepareStatement(getinstance);
            Gson gson = new Gson();
            stmt.setString(1, gson.toJson(tagsa));
            stmt.setString(2, name);
            stmt.executeUpdate();

            stmt.close();
            conn.close();
        } catch (Exception e) {
            throw e;
        }

    }
}


