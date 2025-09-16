package com.seoul.login;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// 15초마다 한번씩 시도 (wrong:wrong-secret)
public class NotMove {
    static final Counter producedMessages = Counter.build()
            .name("kafka_produced_messages_total")
            .help("Number of messages successfully produced")
            .register();

    static final Counter produceErrors = Counter.build()
            .name("kafka_produce_errors_total")
            .help("Number of produce errors")
            .register();

    public static void main(String[] args) {
        // Start Prometheus HTTP server for metrics
        try {
            new HTTPServer(new InetSocketAddress("0.0.0.0", 1235),
                    CollectorRegistry.defaultRegistry,
                    true);
            System.out.println("Prometheus metrics HTTP server started on port 1235");
        } catch (Exception e) {
            System.err.println("Failed to start Prometheus HTTP server: " + e.getMessage());
        }

        final String bootstrap = "15.164.187.115:9092,43.200.197.192:9092,13.209.235.184:9092";
        final String securityProtocol = "SASL_PLAINTEXT";
        final String saslMech = "SCRAM-SHA-512";
        final String topic = "audit-topic";

        // 세 가지 계정 정보
        String[][] creds = new String[][]{

                {"wrong", "wrong-secret"}
        };

        // ===== Scheduler: 15초마다 한번씩 시도 =====
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        Runnable task = () -> {
            for (String[] cred : creds) {
                final String user = cred[0];
                final String pass = cred[1];

                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");

                props.put("security.protocol", securityProtocol);
                props.put(SaslConfigs.SASL_MECHANISM, saslMech);
                props.put(SaslConfigs.SASL_JAAS_CONFIG, jaas(user, pass));
                props.put(ProducerConfig.ACKS_CONFIG, "all");

                System.out.printf("\n[PRODUCER] Trying as %s%n", user);
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                    String key = "key-" + System.currentTimeMillis();
                    String value = "Hello AuditTopic from " + user;
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                    producer.send(record, (metadata, exception) -> {
                        if (exception == null) {
                            producedMessages.inc();
                            System.out.printf("[PRODUCE] user=%s topic=%s partition=%d offset=%d key=%s value=%s%n",
                                    user, metadata.topic(), metadata.partition(), metadata.offset(), key, value);
                        } else {
                            produceErrors.inc();
                            System.err.printf("[PRODUCE-ERROR] user=%s error=%s%n",
                                    user, exception.getMessage());
                        }
                    });
                    producer.flush();
                } catch (Exception e) {
                    System.err.printf("[PRODUCER] user=%s init ERROR: %s%n", user, e.getMessage());
                }
            }
        };

        // initialDelay=0, period=15 seconds
        scheduler.scheduleAtFixedRate(task, 0, 15, TimeUnit.SECONDS);

        // Graceful shutdown on JVM exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down scheduler...");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException ignored) {
                scheduler.shutdownNow();
            }
        }));
    }

    private static String jaas(String user, String pass) {
        return String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                user, pass);
    }
}
