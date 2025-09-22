package com.seoul.produce;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class LocalProduce {

    private static final String TOPIC_AUTH_FAILURE    = "certified-2time";       // /api/kafka/auth_failure
    private static final String TOPIC_AUTH_SUSPICIOUS = "certified-notMove";    // /api/kafka/auth_suspicious
    private static final String TOPIC_AUTH_SYSTEM     = "cluster-level-false";   // /api/kafka/auth_system
    private static final String TOPIC_AUTH_RESOURCE   = "system-level-false";    // /api/kafka/auth_resource

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter KST_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS 'KST'");

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();

    public LocalProduce(Properties props) {
        // Ensure serializers if not provided in props
        props.putIfAbsent("key.serializer", StringSerializer.class.getName());
        props.putIfAbsent("value.serializer", StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    public void close() {
        if (producer != null) producer.close();
    }

    private static String randomPublicIp() {
        // Avoid private ranges; keep it simple with some public-looking blocks
        int a = pick(Arrays.asList(13, 18, 34, 35, 43, 52, 54, 66, 75, 99, 101, 103));
        int b = ThreadLocalRandom.current().nextInt(0, 256);
        int c = ThreadLocalRandom.current().nextInt(0, 256);
        int d = ThreadLocalRandom.current().nextInt(1, 255);
        return a + "." + b + "." + c + "." + d;
    }

    private static <T> T pick(List<T> list) {
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }

    private static String nowKst() {
        return ZonedDateTime.now(KST).format(KST_FMT);
    }

    private static String nowKstIso() {
        // ISO-like with offset converted to KST
        return ZonedDateTime.now(KST).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS z"));
    }

    private static String localIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }

    // 1) /api/kafka/auth_failure → certified-2time
    private String buildAuthFailureJson() {
        ObjectNode root = mapper.createObjectNode();
        root.put("id", UUID.randomUUID().toString());
        root.put("client_ip", randomPublicIp());
        root.put("alert_time_kst", nowKst());
        root.put("alert_type", "FREQUENT_FAILURES");
        int count = ThreadLocalRandom.current().nextInt(2, 6); // 2~5
        root.put("description", "IP " + root.get("client_ip").asText() + " has failed authentication " + count + " times within 10 seconds.");
        root.put("failure_count", count);
        return root.toString();
    }

    // 2) /api/kafka/auth_suspicious → certified-notMove
    private String buildAuthSuspiciousJson() {
        ObjectNode root = mapper.createObjectNode();
        root.put("id", UUID.randomUUID().toString());
        root.put("client_ip", randomPublicIp());
        root.put("alert_time_kst", nowKst());
        root.put("alert_type", "INACTIVITY_AFTER_FAILURE");
        root.put("description", "IP " + root.get("client_ip").asText() + " showed no activity for 10 seconds after a single authentication failure.");
        root.put("failure_count", 1);
        return root.toString();
    }

    // 3) /api/kafka/auth_system → cluster-level-false
    private String buildAuthSystemJson() {
        List<String> principals = Arrays.asList("User:admin", "User:ca2", "User:ops", "User:svc-app");
        String principal = pick(principals);

        ObjectNode root = mapper.createObjectNode();
        root.put("id", UUID.randomUUID().toString());
        root.put("event_time_kst", nowKst());
        root.put("processing_time_kst", nowKst());
        root.put("principal", principal);
        root.put("client_ip", localIp());
        root.put("method_name", "mds.Authorize");
        root.put("granted", false);
        root.put("resource_type", "SecurityMetadata");
        root.put("resource_name", "security-metadata");
        root.put("operation", "Describe");
        return root.toString();
    }

    // 4) /api/kafka/auth_resource → system-level-false
    private String buildAuthResourceJson() {
        List<String> principals = Arrays.asList("User:dr", "User:sd", "User:reporter", "User:etl");
        String principal = pick(principals);
        List<String> ops = Arrays.asList("Write", "Read", "Describe");
        String op = pick(ops);
        List<String> topics = Arrays.asList("audit-topic", "login-topic", "unauth-topic", "metrics-topic");
        String topic = pick(topics);

        ObjectNode root = mapper.createObjectNode();
        root.put("id", UUID.randomUUID().toString());
        root.put("event_time_kst", nowKst());
        root.put("processing_time_kst", nowKst());
        root.put("principal", principal);
        root.put("client_ip", randomPublicIp());
        root.put("method_name", "kafka.Produce");
        root.put("granted", false);
        root.put("resource_type", "Topic");
        root.put("resource_name", topic);
        root.put("operation", op);
        return root.toString();
    }

    private void send(String topic, String key, String value) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        Future<RecordMetadata> fut = producer.send(record);
        RecordMetadata meta = fut.get();
        System.out.printf("→ sent topic=%s partition=%d offset=%d key=%s\n", meta.topic(), meta.partition(), meta.offset(), key);
    }

    public void run(int iterations, long intervalMs) throws Exception {
        for (int i = 0; i < iterations || iterations < 0; i++) {
            // 1 → 2 → 3 → 4 순서 보장
            String m1 = buildAuthFailureJson();
            send(TOPIC_AUTH_FAILURE,  extractKey(m1), m1);

            String m2 = buildAuthSuspiciousJson();
            send(TOPIC_AUTH_SUSPICIOUS, extractKey(m2), m2);

            String m3 = buildAuthSystemJson();
            send(TOPIC_AUTH_SYSTEM,    extractKey(m3), m3);

            String m4 = buildAuthResourceJson();
            send(TOPIC_AUTH_RESOURCE,  extractKey(m4), m4);

            if (intervalMs > 0) Thread.sleep(intervalMs);
        }
    }

    private String extractKey(String json) {
        try {
            ObjectNode n = (ObjectNode) mapper.readTree(json);
            if (n.has("client_ip")) return n.get("client_ip").asText();
            if (n.has("principal")) return n.get("principal").asText();
            return n.get("id").asText();
        } catch (Exception e) {
            return UUID.randomUUID().toString();
        }
    }

    private static Properties loadProps(String propsPath) throws IOException {
        Properties p = new Properties();

        // 1) Try explicit file path (when -Dprops is an absolute/relative path)
        try (FileInputStream fis = new FileInputStream(propsPath)) {
            p.load(fis);
            return p;
        } catch (IOException ignore) {
            // fall through to classpath
        }

        // 2) Try classpath resource (e.g., src/main/resources/producer.properties)
        try (InputStream is = LocalProduce.class.getClassLoader().getResourceAsStream(propsPath)) {
            if (is != null) {
                p.load(is);
                return p;
            }
        }

        // 3) Try default classpath name if user passed only a filename that isn't found as a file
        try (InputStream is = LocalProduce.class.getClassLoader().getResourceAsStream("producer.properties")) {
            if (is != null) {
                p.load(is);
                return p;
            }
        }

        throw new IOException("Cannot find properties file: " + propsPath + " (also tried classpath resources)");
    }

    public static void main(String[] args) throws Exception {
        // CLI args
        String propsPath = System.getProperty("props", "producer.properties");
        int iterations = Integer.parseInt(System.getProperty("iterations", "-1")); // -1: infinite
        long intervalMs = Long.parseLong(System.getProperty("intervalMs", "1000"));

        Properties p = loadProps(propsPath);

        // 최소 필수 값 안내
        if (!p.containsKey("bootstrap.servers")) {
            System.err.println("Missing 'bootstrap.servers' in properties file");
            System.exit(1);
        }

        LocalProduce app = new LocalProduce(p);
        try {
            System.out.printf("Starting producer (iterations=%d, intervalMs=%d)\n", iterations, intervalMs);
            app.run(iterations, intervalMs);
        } finally {
            app.close();
        }
    }
}