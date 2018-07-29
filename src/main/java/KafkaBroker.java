import java.util.*;
import java.io.File;
import java.io.FileNotFoundException;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import java.io.StringReader;
import java.sql.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.*;
import org.apache.commons.lang3.*;
import spark.Request;
import spark.Response;
import javax.servlet.http.HttpServletResponse;
import static java.lang.Math.toIntExact;
import static spark.Spark.*;

public class KafkaBroker {

    static class Topic {
        public String topic;
        public String organization;
        public Long retentionms;
        public Long partitions;
        public Long replicas;
        public String cleanuppolicy;
        public String description;
    }

    static class Consumergroup {
        public String consumergroupname;
        public String username;
        public String topic;
    }

    public static void main(String[] args) {
        Logger.getRootLogger().setLevel(Level.OFF);
        try {
            Connection conn = connectToDatabaseOrDie();
            File sql = new File("create.sql");
            executeSqlScript(conn, sql);
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        port(Integer.parseInt(System.getenv("PORT")));

        post("/v1/kafka/cluster/:cluster/user/:username/topic/:topic/consumergroup/rotate", (req, res) -> {
            String newgroup = "";
            try {
                newgroup = rotateConsumergroup(req.params(":cluster"), req.params(":username"), req.params(":topic"));
            } catch (Exception e) {
                res.status(getStatusFromMessage(e.getMessage()));
                return "{\"result\":\"" + "error" + "\", \"message\":\"" + fixException(e) + "\"}";
            }
            res.status(200);
            return "{\"rotate\":\"" + req.params(":topic") + "\", \"message\":\"" + newgroup + "\"}";
        });

        get("/v1/kafka/cluster/:cluster/credentials/:username", (req, res) -> {
            String username = req.params("username");
            String cluster = req.params("cluster");
            JSONObject main = new JSONObject();
            try {
                Map<String, String> credentials = getCredentials(username, cluster);
                main.putAll(credentials);
            } catch (Exception e) {
                e.printStackTrace();
                res.status(getStatusFromMessage(e.getMessage()));
                return "{\"result\":\"" + "error" + "\", \"message\":\"" + fixException(e) + "\"}";
            }
            res.status(200);
            return main.toJSONString();
        });

        post("/v1/kafka/cluster/:cluster/user", (req, res) -> {
            String cluster = req.params("cluster");
            String[] up;
            String username = "";
            String password = "";
            try {
                up = claimUser(cluster);
                username = up[0];
                password = up[1];
            } catch (Exception e) {
                e.printStackTrace();
                res.status(getStatusFromMessage(e.getMessage()));
                return "{\"result\":\"" + "error" + "\", \"message\":\"" + fixException(e) + "\"}";
            }
            res.status(200);
            return "{\"username\":\"" + username + "\", \"password\":\"" + password + "\"}";
        });

        get("/v1/kafka/cluster/:cluster/topic/:topic", (req, res) -> {
            JSONObject main = new JSONObject();
            String topictoget = req.params(":topic");
            try {
                String cluster = req.params("cluster");
                Topic n = getTopic(topictoget, cluster);
                JSONObject t = new JSONObject();
                JSONObject tconfig = new JSONObject();
                tconfig.put("partitions", n.partitions);
                tconfig.put("replicas", n.replicas);
                tconfig.put("retentionms", n.retentionms);
                tconfig.put("organization", n.organization);
                tconfig.put("cleanuppolicy", n.cleanuppolicy);
                t.put("config", tconfig);
                t.put("name", n.topic);
                t.put("description", n.description);
                t.put("organization", n.organization);
                main.put("topic", t);
            } catch (Exception e) {
                e.printStackTrace();
                res.status(getStatusFromMessage(e.getMessage()));
                return "{\"result\":\"" + "error" + "\", \"message\":\"" + fixException(e) + "\"}";
            }
            res.status(200);
            return main.toJSONString();
        });

        get("/v1/kafka/cluster/:cluster/topics", (req, res) -> {
            JSONArray topiclist = new JSONArray();
            try {
                String cluster = req.params("cluster");
                Vector<Topic> topics = getTopics(cluster);
                java.util.Iterator ti = topics.iterator();
                while (ti.hasNext()) {
                    JSONObject main = new JSONObject();
                    Topic n = (Topic) ti.next();
                    JSONObject t = new JSONObject();
                    JSONObject tconfig = new JSONObject();
                    tconfig.put("partitions", n.partitions);
                    tconfig.put("replicas", n.replicas);
                    tconfig.put("retentionms", n.retentionms);
                    tconfig.put("organization", n.organization);
                    tconfig.put("cleanuppolicy", n.cleanuppolicy);
                    t.put("config", tconfig);
                    t.put("name", n.topic);
                    t.put("description", n.description);
                    t.put("organization", n.organization);
                    main.put("topic", t);
                    topiclist.add(main);
                }
            } catch (Exception e) {
                e.printStackTrace();
                res.status(getStatusFromMessage(e.getMessage()));
                return "{\"result\":\"" + "error" + "\", \"message\":\"" + fixException(e) + "\"}";

            }
            res.status(200);
            return topiclist.toJSONString();
        });

        get("/v1/kafka/cluster/:cluster/consumergroups", (req, res) -> {
            JSONArray list = new JSONArray();
            try {
                String cluster = req.params("cluster");
                Vector<Consumergroup> consumergroups = getConsumergroups(cluster);
                java.util.Iterator<Consumergroup> cgi = consumergroups.iterator();
                while (cgi.hasNext()) {
                    Consumergroup n = cgi.next();
                    JSONObject cgmain = new JSONObject();
                    JSONObject cg = new JSONObject();
                    cg.put("consumergroupname", n.consumergroupname);
                    cg.put("username", n.username);
                    cg.put("topic", n.topic);
                    cgmain.put("consumergroup", cg);
                    list.add(cgmain);
                }
            } catch (Exception e) {
                e.printStackTrace();
                res.status(getStatusFromMessage(e.getMessage()));
                return "{\"result\":\"" + "error" + "\", \"message\":\"" + fixException(e) + "\"}";
            }
            res.status(200);
            return list.toJSONString();
        });

        get("/v1/kafka/cluster/:cluster/topic/:topic/consumergroups", (req, res) -> {
            JSONArray list = new JSONArray();
            try {
                String cluster = req.params(":cluster");
                String topic = req.params(":topic");

                Vector<Consumergroup> consumergroups = getConsumergroupsForTopic(cluster, topic);
                java.util.Iterator<Consumergroup> cgi = consumergroups.iterator();
                while (cgi.hasNext()) {
                    Consumergroup n = cgi.next();
                    JSONObject cgmain = new JSONObject();
                    JSONObject cg = new JSONObject();
                    cg.put("consumergroupname", n.consumergroupname);
                    cg.put("username", n.username);
                    cg.put("topic", n.topic);
                    cgmain.put("consumergroup", cg);
                    list.add(cgmain);
                }
            } catch (Exception e) {
                e.printStackTrace();
                res.status(getStatusFromMessage(e.getMessage()));
                return "{\"result\":\"" + "error" + "\", \"message\":\"" + fixException(e) + "\"}";

            }
            res.status(200);
            return list.toJSONString();
        });
        put("/v1/kafka/cluster/:cluster/acl/user/:user/topic/:topic/role/:role", (req, res) -> {
            String result = "";
            String cluster = "";
            String topic = "";
            String user = "";
            String role = "";
            try {

                cluster = req.params(":cluster");
                topic = req.params(":topic");
                user = req.params(":user");
                role = req.params(":role");
                if (!role.equalsIgnoreCase("producer") && !role.equalsIgnoreCase("consumer")) {
                    res.status(400);
                    return "{\"result\":\"" + "error" + "\", \"message\":\""
                            + "role must be either producer or consumer" + "\"}";
                }
                result = grantUserTopicRole(cluster, user, topic, role);
            } catch (Exception e) {
                e.printStackTrace();
                res.status(500);
                return "{\"result\":\"" + "error" + "\", \"message\":\"" + fixException(e) + "\"}";
            }
            res.status(200);
            return "{\"acl\":\"" + user + "/" + topic + "/" + role + "\", \"message\":\"" + result + "\"}";
        });

        post("/v1/kafka/cluster/:cluster/topic", (req, res) -> {
            String topicname = "";
            String result = "";
            try {
                String cluster = req.params("cluster");
                String body = req.body();
                long replicas = Long.parseLong(System.getenv(cluster.toUpperCase() + "_DEFAULT_REPLICAS"));
                JSONParser jsonParser = new JSONParser();
                StringReader sr = new StringReader(body);
                JSONObject jsonObject = (JSONObject) jsonParser.parse(sr);
                JSONObject topicelement = (JSONObject) jsonObject.get("topic");
                topicname = (String) topicelement.get("name");
                String description = (String) topicelement.get("description");
                String organization = (String) topicelement.get("organization");
                JSONObject configelements = (JSONObject) topicelement.get("config");
                long partitions = 0;
                Set<String> configkeys = (Set<String>) configelements.keySet();
                if (configkeys.contains("partitions")) {
                    partitions = (Long) configelements.get("partitions");
                } else {
                    partitions = Long.parseLong(System.getenv(cluster.toUpperCase() + "_DEFAULT_PARTITIONS"));
                }
                long retentiontime = 0;
                if (configkeys.contains("retention.ms")) {
                    retentiontime = (Long) configelements.get("retention.ms");
                } else {
                    retentiontime = Long.parseLong(System.getenv(cluster.toUpperCase() + "_DEFAULT_RETENTION"));
                }
                String cleanuppolicy = "delete";
                if (configkeys.contains("cleanup.policy")) {
                    cleanuppolicy = (String) configelements.get("cleanup.policy");
                } else {
                    cleanuppolicy = "delete";
                }

                result = createTopic(cluster, topicname, partitions, replicas, retentiontime, cleanuppolicy,
                        organization, description);

                if (result.equalsIgnoreCase("created")) {
                    res.status(201);
                } else if (result.equalsIgnoreCase("error")) {
                    res.status(500);
                } else if (result.equalsIgnoreCase("exists")) {
                    res.status(200);
                } else {
                    res.status(500);
                }
            } catch (Exception e) {
                e.printStackTrace();
                res.status(getStatusFromMessage(e.getMessage()));
                return "{\"result\":\"" + "error" + "\", \"message\":\"" + fixException(e) + "\"}";

            }
            return "{\"topicrequested\":\"" + topicname + "\", \"message\":\"" + result + "\"}";

        });

        put("/v1/kafka/cluster/:cluster/topic/:topic/retentionms/:retentionms", (req, res) -> {
            String topictoupdate = req.params(":topic");
            String retentionms = req.params(":retentionms");
            String cluster = req.params(":cluster");
            String result = updateTopicRetentionTime(cluster, topictoupdate, Long.parseLong(retentionms));
            if (result.equalsIgnoreCase("updated")) {
                res.status(200);
            } else if (result.equalsIgnoreCase("error")) {
                res.status(500);
            } else if (result.equalsIgnoreCase("does not exist")) {
                res.status(500);
            } else {
                res.status(500);
            }
            return "{\"topic\":\"" + topictoupdate + "\", \"message\":\"" + result + "\"}";

        });

        delete("/v1/kafka/cluster/:cluster/topic/:topic", (req, res) -> {
            String cluster = req.params(":cluster");
            String topictodelete = req.params(":topic");
            String result = deleteTopic(cluster, topictodelete);
            if (result.equalsIgnoreCase("deleted")) {
                res.status(200);
            } else if (result.equalsIgnoreCase("error")) {
                res.status(500);
            } else if (result.equalsIgnoreCase("does not exist")) {
                res.status(500);
            } else {
                res.status(500);
            }
            return "{\"topic\":\"" + topictodelete + "\", \"message\":\"" + result + "\"}";

        });

        after((request, response) -> {
            System.out.println(requestAndResponseInfoToString(request, response));
        });
    }

    public static String updateTopicRetentionTime(String cluster, String topic, Long retentionms) throws Exception {
        String toreturn = "";
        ZkClient zkClient = null;
        ZkUtils zkUtils = null;
        try {
            String zookeeperHosts = System.getenv(cluster.toUpperCase() + "_ZK");
            int sessionTimeOutInMs = 15 * 1000;
            int connectionTimeOutInMs = 10 * 1000;

            zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs,
                    ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);

            String topicName = topic;
            boolean exists = AdminUtils.topicExists(zkUtils, topicName);
            if (exists) {
                Properties topicConfiguration = new Properties();
                topicConfiguration.setProperty("retention.ms", Long.toString(retentionms));
                AdminUtils.changeTopicConfig(zkUtils, topicName, topicConfiguration);
                Connection conn = null;
                conn = connectToDatabaseOrDie();
                String retentionmssql = "update provision_topic set retentionms = ?, updated_timestamp = now() where topic= ? and cluster = ?";
                PreparedStatement ustmt = conn.prepareStatement(retentionmssql);
                ustmt.setLong(1, retentionms);
                ustmt.setString(2, topic);
                ustmt.setString(3, cluster);
                ustmt.executeUpdate();
                ustmt.close();
                conn.close();

                toreturn = "updated";
            } else {
                toreturn = "does not exist";
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            toreturn = "error " + ex.getMessage();
            return toreturn;
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
        return toreturn;
    }

    public static String createTopic(String cluster, String args, long partitions, long replicas, long retentionms,
            String cleanuppolicy, String organization, String description) throws Exception {
        ZkClient zkClient = null;
        ZkUtils zkUtils = null;
        String toreturn = "";
        try {
            String zookeeperHosts = System.getenv(cluster.toUpperCase() + "_ZK");
            int sessionTimeOutInMs = 15 * 1000;
            int connectionTimeOutInMs = 10 * 1000;

            zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs,
                    ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);

            String topicName = args;
            int noOfPartitions = toIntExact(partitions);
            int noOfReplication = toIntExact(replicas);
            Properties topicConfiguration = new Properties();
            topicConfiguration.setProperty("retention.ms", Long.toString(retentionms));
            topicConfiguration.setProperty("cleanup.policy", cleanuppolicy);
            boolean exists = AdminUtils.topicExists(zkUtils, topicName);
            if (!exists) {
                AdminUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplication, topicConfiguration,
                        kafka.admin.RackAwareMode.Disabled$.MODULE$);
                toreturn = "created";
            } else {
                return "exists";

            }
            Connection conn = null;
            conn = connectToDatabaseOrDie();
            String inserttopic = "insert into provision_topic (topic, partitions, replicas, retentionms, cleanuppolicy, cluster, organization,description ) values (?,?,?,?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(inserttopic);
            stmt.setString(1, topicName);
            stmt.setLong(2, partitions);
            stmt.setLong(3, replicas);
            stmt.setLong(4, retentionms);
            stmt.setString(5, cleanuppolicy);
            stmt.setString(6, cluster);
            stmt.setString(7, organization);
            stmt.setString(8, description);

            stmt.executeUpdate();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            throwWithStatus(e, "500");
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
        return toreturn;
    }

    public static Topic getTopic(String topicname, String cluster) throws Exception {

        Topic toreturn = new Topic();

        try {
            Connection conn = null;
            conn = connectToDatabaseOrDie();
            String gettopics = "select topic, partitions, replicas, retentionms, cleanuppolicy, organization, description from provision_topic where cluster = ? and topic = ? limit 1";
            PreparedStatement stmt = conn.prepareStatement(gettopics);
            stmt.setString(1, cluster);
            stmt.setString(2, topicname);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                toreturn.topic = rs.getString(1);
                toreturn.partitions = rs.getLong(2);
                toreturn.replicas = rs.getLong(3);
                toreturn.retentionms = rs.getLong(4);
                toreturn.cleanuppolicy = rs.getString(5);
                toreturn.organization = rs.getString(6);
                toreturn.description = rs.getString(7);
            }
            stmt.close();
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return toreturn;
    }

    public static Vector<Topic> getTopics(String cluster) throws Exception {

        java.util.Vector<Topic> toreturn = new Vector<Topic>();
        try {
            Connection conn = null;
            conn = connectToDatabaseOrDie();
            String gettopics = "select topic, partitions, replicas, retentionms, cleanuppolicy, organization, description from provision_topic where cluster = ? order by organization, topic";
            PreparedStatement stmt = conn.prepareStatement(gettopics);
            stmt.setString(1, cluster);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                Topic topic = new Topic();
                topic.topic = rs.getString(1);
                topic.partitions = rs.getLong(2);
                topic.replicas = rs.getLong(3);
                topic.retentionms = rs.getLong(4);
                topic.cleanuppolicy = rs.getString(5);
                topic.organization = rs.getString(6);
                topic.description = rs.getString(7);
                toreturn.add(topic);
            }
            stmt.close();
        } catch (Exception e) {
            throwWithStatus(e, "500");
        }
        return toreturn;
    }

    public static String setACL(String cluster, String topic, String user, String role, String consumergroupname)
            throws Exception {
        String toreturn = "";
        try {
            String zk = System.getenv(cluster.toUpperCase() + "_ZK");
            if (role.equalsIgnoreCase("producer")) {
                String[] cmdPArm = { "--authorizer-properties", "zookeeper.connect=" + zk, "--add", "--allow-principal",
                        "User:" + user, "--" + role, "--topic", topic };
                kafka.admin.AclCommand.main(cmdPArm);
                toreturn = "created";
            }

            if (role.equalsIgnoreCase("consumer")) {
                String[] cmdPArm = { "--authorizer-properties", "zookeeper.connect=" + zk, "--add", "--allow-principal",
                        "User:" + user, "--" + role, "--group", "*", "--topic", topic };
                kafka.admin.AclCommand.main(cmdPArm);
                toreturn = "created";
            }
        } catch (Exception e) {
            throwWithStatus(e, "500");
        }
        return toreturn;
    }

    public static Vector<Consumergroup> getConsumergroups(String cluster) throws Exception {

        Vector<Consumergroup> toreturn = new Vector<Consumergroup>();
        try {
            Connection conn = null;
            conn = connectToDatabaseOrDie();
            String gettopics = "select username, topic, consumergroupname from consumergroups where cluster = ?";
            PreparedStatement stmt = conn.prepareStatement(gettopics);
            stmt.setString(1, cluster);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                Consumergroup cg = new Consumergroup();
                cg.username = rs.getString(1);
                cg.topic = rs.getString(2);
                cg.consumergroupname = rs.getString(3);
                toreturn.add(cg);
            }
            stmt.close();
        } catch (Exception e) {
            throwWithStatus(e, "500");
        }
        return toreturn;
    }

    public static String deleteTopic(String cluster, String args) {
        String zookeeperHosts = System.getenv(cluster.toUpperCase() + "_ZK");
        String toreturn = "";
        ZkClient zkClient = null;
        ZkUtils zkUtils = null;
        try {
            int sessionTimeOutInMs = 15 * 1000;
            int connectionTimeOutInMs = 10 * 1000;

            zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs,
                    ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);

            String topicName = args;
            System.out.println("about to delete " + topicName);
            boolean exists = AdminUtils.topicExists(zkUtils, topicName);
            if (exists) {
                AdminUtils.deleteTopic(zkUtils, topicName);
                toreturn = "deleted";
            } else {
                toreturn = "does not exist";
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            toreturn = "error - " + ex.getMessage();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
        return toreturn;
    }

    private static Connection connectToDatabaseOrDie() throws Exception {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            String url = System.getenv("BROKERDB");
            conn = DriverManager.getConnection(url, System.getenv("BROKERDBUSER"), System.getenv("BROKERDBPASS"));
        } catch (Exception e) {
            throwWithStatus(e, "500");
        }
        return conn;
    }

    private static String[] claimUser(String cluster) throws Exception {
        String username = "";
        String password = "";
        Connection conn = null;
        conn = connectToDatabaseOrDie();
        try {
            String claimquery = "select username,password from provision_user where claimed=false and cluster = ? limit 1";
            PreparedStatement stmt = conn.prepareStatement(claimquery);
            stmt.setString(1, cluster);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                username = rs.getString(1);
                password = rs.getString(2);
            }
            stmt.close();
            String claimupdate = "update provision_user set claimed = true, claimed_timestamp = now() where username= ? and cluster = ?";
            PreparedStatement ustmt = conn.prepareStatement(claimupdate);
            ustmt.setString(1, username);
            ustmt.setString(2, cluster);
            ustmt.executeUpdate();
            ustmt.close();
            conn.close();
        } catch (Exception e) {
            throwWithStatus(e, "500");
        }

        return new String[] { username, password };

    }

    private static String grantUserTopicRole(String cluster, String username, String topic, String role)
            throws Exception {
        String toreturn = "";
        try {
            UUID idOne = UUID.randomUUID();
            String consumergroupname = username + "-" + idOne.toString().split("-")[0];
            toreturn = setACL(cluster, topic, username, role, consumergroupname);
            Connection conn = null;
            conn = connectToDatabaseOrDie();
            String insertacl = "insert into provision_acl (userid,topicid,role, cluster) values (?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(insertacl);
            stmt.setString(1, getUseridByName(cluster, username));
            stmt.setString(2, getTopicidByName(cluster, topic));
            stmt.setString(3, role);
            stmt.setString(4, cluster);
            stmt.executeUpdate();
            stmt.close();

            if (role.equalsIgnoreCase("consumer")) {
                String insertcg = "insert into provision_consumergroup (userid, topicid, consumergroupname, active, cluster) values (?,?,?,?,?)";
                PreparedStatement stmt2 = conn.prepareStatement(insertcg);
                stmt2.setString(1, getUseridByName(cluster, username));
                stmt2.setString(2, getTopicidByName(cluster, topic));
                stmt2.setString(3, consumergroupname);
                stmt2.setBoolean(4, true);
                stmt2.setString(5, cluster);
                stmt2.executeUpdate();
                stmt2.close();
            }
            conn.close();
        } catch (Exception e) {
            throwWithStatus(e, "500");
        }
        return toreturn;
    }

    public static String[] removeNulls(String args[]) {
        List<String> list = new ArrayList<String>();

        for (String s : args) {
            if (s != null && s.length() > 0) {
                list.add(s);
            }
        }

        return list.toArray(new String[list.size()]);
    }

    private static String getTopicidByName(String cluster, String name) throws Exception{
        String id = "";

        try {
            Connection conn = null;
            conn = connectToDatabaseOrDie();
            String getinstance = "select topicid from provision_topic where topic = ? and cluster = ?";
            PreparedStatement stmt = conn.prepareStatement(getinstance);
            stmt.setString(1, name);
            stmt.setString(2, cluster);
            ResultSet rs = stmt.executeQuery();
            int rows = 0;
            while (rs.next()) {
                rows++;
                id = rs.getString(1);
            }
            stmt.close();
            conn.close();
            if (rows != 1) {
                //return "":
                throw new CustomException("topic does not exist");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return id;
    }

    private static String getUseridByName(String cluster, String name) {
        String id = "";

        try {
            Connection conn = null;
            conn = connectToDatabaseOrDie();
            String getinstance = "select userid from provision_user where username = ? and cluster = ?";
            PreparedStatement stmt = conn.prepareStatement(getinstance);
            stmt.setString(1, name);
            stmt.setString(2, cluster);
            ResultSet rs = stmt.executeQuery();
            int rows = 0;
            while (rs.next()) {
                rows++;
                id = rs.getString(1);
            }
            stmt.close();
            conn.close();
            if (rows != 1) {
                return "";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }

    private static Map<String, String> getCredentials(String username, String cluster) throws Exception {
        String password = "";
        Map<String, String> credentials = new HashMap<String, String>();
        java.util.Vector<String> producerlist = new Vector<String>();
        java.util.Vector<String> consumerlist = new Vector<String>();

        try {
            Connection conn = null;
            conn = connectToDatabaseOrDie();
            String getinstance = "select  password, topic, role, consumergroupname from credentials where username = ? and cluster = ?";
            PreparedStatement stmt = conn.prepareStatement(getinstance);
            stmt.setString(1, username);
            stmt.setString(2, cluster);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                password = rs.getString(1);
                String topic = rs.getString(2);
                String role = rs.getString(3);
                String consumergroupname = rs.getString(4);

                if (role.equalsIgnoreCase("producer")) {
                    producerlist.add(topic);
                }
                if (role.equalsIgnoreCase("consumer")) {
                    consumerlist.add(topic);
                    credentials.put("KAFKA_CG_" + topic.toUpperCase().replace('.', '_'), consumergroupname);

                }

            }
            stmt.close();
            conn.close();
            credentials.put("KAFKA_HOSTNAME", System.getenv(cluster.toUpperCase() + "_KAFKA_HOSTNAME"));
            credentials.put("KAFKA_PORT", System.getenv(cluster.toUpperCase() + "_KAFKA_PORT"));
            credentials.put("KAFKA_LOCATION", System.getenv(cluster.toUpperCase() + "_KAFKA_LOCATION"));
            credentials.put("KAFKA_AVRO_REGISTRY_LOCATION",
                    System.getenv(cluster.toUpperCase() + "_KAFKA_AVRO_REGISTRY_LOCATION"));
            credentials.put("KAFKA_AVRO_REGISTRY_HOSTNAME",
                    System.getenv(cluster.toUpperCase() + "_KAFKA_AVRO_REGISTRY_HOSTNAME"));
            credentials.put("KAFKA_AVRO_REGISTRY_PORT",
                    System.getenv(cluster.toUpperCase() + "_KAFKA_AVRO_REGISTRY_PORT"));
            credentials.put("KAFKA_USERNAME", username);
            credentials.put("KAFKA_PASSWORD", password);
            credentials.put("KAFKA_RESOURCENAME", cluster);
            credentials.put("KAFKA_SASL_MECHANISM", "PLAIN");
            credentials.put("KAFKA_SECURITY_PROTOCOL", "SASL_SSL");
            if (producerlist.size() > 0) {
                String plist = StringUtils.join(producerlist, ',');
                credentials.put("KAFKA_PRODUCE_LIST", plist);
            }
            if (consumerlist.size() > 0) {
                String clist = StringUtils.join(consumerlist, ',');
                credentials.put("KAFKA_CONSUME_LIST", clist);
            }

        } catch (Exception e) {
            throw e;
        }
        return credentials;
    }

    private static Vector<Consumergroup> getConsumergroupsForTopic(String cluster, String topic) throws Exception {
        Vector<Consumergroup> toreturn = new Vector<Consumergroup>();

        try {
            getTopicidByName(cluster, topic);
            Connection conn = null;
            conn = connectToDatabaseOrDie();
            String gettopics = "select username, topic, consumergroupname from consumergroups where cluster = ? and topic = ?";
            PreparedStatement stmt = conn.prepareStatement(gettopics);
            stmt.setString(1, cluster);
            stmt.setString(2, topic);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                Consumergroup cg = new Consumergroup();
                cg.username = rs.getString(1);
                cg.topic = rs.getString(2);
                cg.consumergroupname = rs.getString(3);
                toreturn.add(cg);
            }
            stmt.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throwWithStatus(e, "500");
        }
        return toreturn;
    }

    private static String requestAndResponseInfoToString(Request request, Response response) {
        StringBuilder sb = new StringBuilder();
        sb.append(request.requestMethod());
        sb.append(" " + request.url());
        HttpServletResponse raw = response.raw();
        sb.append(" status=" + raw.getStatus());
        String body = request.body();
        String newbody = body.trim().replaceAll(" +", " ");
        sb.append(" " + newbody);
        return sb.toString();
    }

    private static String rotateConsumergroup(String cluster, String username, String topic) throws Exception {
        UUID idOne = UUID.randomUUID();
        String consumergroupname = username + "-" + idOne.toString().split("-")[0];
        try {

            String userid = getUseridByName(cluster, username);
            String topicid = getTopicidByName(cluster, topic);

            Connection conn = null;
            conn = connectToDatabaseOrDie();
            String deactivate = "update provision_consumergroup set active = false, updated_timestamp = now() where cluster = ? and userid = ? and topicid = ? and active = true";
            PreparedStatement ustmt = conn.prepareStatement(deactivate);
            ustmt.setString(1, cluster);
            ustmt.setString(2, userid);
            ustmt.setString(3, topicid);
            ustmt.executeUpdate();
            int updatecount = ustmt.getUpdateCount();
            System.out.println(updatecount);
            if (updatecount != 1) {
                ustmt.close();
                throw new CustomException(
                        "Update count was not equal to 1.  Please verify values of user, topic, and cluser.  Or user/topic/role was not a consumer");
            }

            String insertcg = "insert into provision_consumergroup (userid, topicid, consumergroupname, active, cluster) values (?,?,?,?,?)";
            PreparedStatement stmt2 = conn.prepareStatement(insertcg);
            stmt2.setString(1, userid);
            stmt2.setString(2, topicid);
            stmt2.setString(3, consumergroupname);
            stmt2.setBoolean(4, true);
            stmt2.setString(5, cluster);
            stmt2.executeUpdate();
            int insertcount = stmt2.getUpdateCount();
            System.out.println(insertcount);
            if (updatecount != 1) {
                ustmt.close();
                throw new CustomException(
                        "Insert count was not equal to 1.  Please verify values of user, topic, and cluser.  Or user/topic/role was not a consumer");
            }
            stmt2.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
            throwWithStatus(e, "500");
        }
        return consumergroupname;
    }

    private static String fixException(Exception e) {
        String stripped =  e.getMessage().replace("\"", "").replace('\n', '.');
        String[] splitted = stripped.split("~");

        return splitted[splitted.length-1];
    }

    private static void throwWithStatus(Exception e, String status) throws Exception {
        throw new Exception("status~" + status + "~" + e.getMessage(), e);
    }

    private static int getStatusFromMessage(String message) {
        int toreturn = 501;
        if (message.startsWith("status")) {
            toreturn = Integer.parseInt(message.split("~")[1]);
        }
        return toreturn;
    }

    private static void executeSqlScript(Connection conn, File inputFile) throws Exception {

        String delimiter = ";";
        Scanner scanner;
        try {
            scanner = new Scanner(inputFile).useDelimiter(delimiter);
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
            return;
        }

        Statement currentStatement = null;
        while (scanner.hasNext()) {
            String rawStatement = scanner.next() + delimiter;
            try {
                currentStatement = conn.createStatement();
                currentStatement.execute(rawStatement);
            } catch (SQLException e) {
                e.printStackTrace();
                throw e;
            } finally {
                if (currentStatement != null) {
                    try {
                        currentStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        throw e;
                    }
                }
                currentStatement = null;
            }
        }
        scanner.close();
    }
}
