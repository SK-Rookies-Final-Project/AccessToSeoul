
package com.seoul.consume;

import io.prometheus.client.exporter.HTTPServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class ConsumeAuditTopic {
    private static final String DEFAULT_SECURITY_PROTOCOL = "SASL_PLAINTEXT";
    private static final String DEFAULT_SASL_MECH = "SCRAM-SHA-512";

    public static void main(String[] args) {
        // Start Prometheus HTTP server for metrics
        try {
            new HTTPServer(1236);
            System.out.println("Prometheus metrics HTTP server started on port 1235");
        } catch (Exception e) {
            System.err.println("Failed to start Prometheus HTTP server: " + e.getMessage());
        }

        final String bootstrap = "15.164.187.115:9092,43.200.197.192:9092,13.209.235.184:9092";
        final String securityProtocol = "SASL_PLAINTEXT";
        final String saslMech = "SCRAM-SHA-512";
        final String[][] creds = new String[][]{
                {"dr", "dr-secret"},
                {"dw", "dw-secret"},
                {"wrong", "wrong-secret"}
        };
        final String auditTopic = "audit-topic";

        while (true) {
            for (String[] cred : creds) {
                final String auditUser = cred[0];
                final String auditPass = cred[1];

                Properties p = new Properties();
                p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
                p.put(ConsumerConfig.GROUP_ID_CONFIG, "audit-topic-consume-group-" + auditUser);
                p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
                p.put("security.protocol", securityProtocol);
                p.put(SaslConfigs.SASL_MECHANISM, saslMech);
                p.put(SaslConfigs.SASL_JAAS_CONFIG, jaas(auditUser, auditPass));

                System.out.printf("\n[AUDIT-CONSUMER] Trying as %s%n", auditUser);
                try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
                    c.subscribe(Collections.singletonList(auditTopic));
                    ConsumerRecords<String, String> records = c.poll(Duration.ofSeconds(3));
                    for (ConsumerRecord<String, String> r : records) {
                        System.out.printf("[AUDIT] off=%d key=%s value=%s%n", r.offset(), r.key(), r.value());
                    }
                } catch (Exception e) {
                    System.out.printf("[AUDIT-CONSUMER] %s ERROR: %s%n", auditUser, e.getMessage());
                }

                try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            }
        }
    }

    private static String jaas(String user, String pass) {
        return String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";", user, pass);
    }


    private static String pick(String src, String start, String end) {
        int i = src.indexOf(start);
        if (i < 0) return null;
        i += start.length();
        int j = end.equals("\"") ? src.indexOf('"', i) : src.indexOf(end, i);
        if (j < 0) j = Math.min(i + 120, src.length());
        return src.substring(i, Math.min(j, src.length()));
    }

    private static String nv(String s) { return s == null ? "-" : s; }
}
