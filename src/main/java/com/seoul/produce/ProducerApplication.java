package com.seoul.produce;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class ProducerApplication {

    static class User {
        final String name;
        final String password; // null이면 고의로 틀린 비번을 넣어 Auth 실패 유도
        final boolean expectWriteAllowed; // 데모 메시지 표시에만 사용

        User(String name, String password, boolean expectWriteAllowed) {
            this.name = name;
            this.password = password;
            this.expectWriteAllowed = expectWriteAllowed;
        }
    }


    static final List<User> USERS = List.of(
            new User("admin", "admin-secret", true),
            new User("dw",    "dw-secret",    true),
            new User("ro",    "ro-secret",    false),
            new User("ua",    "ua-secret",    false),
            new User("dm",    "dm-secret", false),
            new User("dr",    "dr-secret", false)
    );

    public static void main(String[] args) throws Exception {
        final String bootstrap = System.getenv().getOrDefault(
                "BOOTSTRAP",
                // 서울 브로커들 주소
//                "15.164.187.115:9092,43.200.197.192:9092,13.209.235.184:9092"
                "15.164.187.115:9092"
        );
        final String topic = System.getenv().getOrDefault("TOPIC", "audit-topic");
        final long intervalMs = Long.parseLong(System.getenv().getOrDefault("INTERVAL_MS", "1000"));

        long loop = 0;
        System.out.printf("BOOTSTRAP=%s, TOPIC=%s%n", bootstrap, topic);

        // 유저별 Producer 풀(필요 시 재사용)
        Map<String, KafkaProducer<String,String>> producers = new HashMap<>();

        try {
            while (true) {
                User u = pickRandom(USERS);
                KafkaProducer<String,String> p = producers.computeIfAbsent(u.name, k -> newProducer(bootstrap, u));

                String key = "user-" + u.name;
                String value = sampleJson(loop, u.name);

                ProducerRecord<String,String> rec = new ProducerRecord<>(topic, key, value);

                try {
                    RecordMetadata md = p.send(rec).get();
                    System.out.printf("[OK    ] user=%s -> %s p=%d o=%d%n",
                            u.name, topic, md.partition(), md.offset());
                } catch (Exception sendEx) {
                    Throwable cause = sendEx.getCause() != null ? sendEx.getCause() : sendEx;

                    if (cause instanceof AuthorizationException) {
                        System.out.printf("[DENIED] user=%s write to %s  (no ACL)%n", u.name, topic);
                    } else if (cause instanceof AuthenticationException ||
                            cause.getMessage() != null && cause.getMessage().contains("Authentication failed")) {
                        System.out.printf("[AUTH-FAIL] user=%s authentication failed%n", u.name);
                    } else {
                        System.out.printf("[ERROR ] user=%s send failed: %s%n", u.name, cause.toString());
                    }
                }

                loop++;
                Thread.sleep(intervalMs);
            }
        } finally {
            producers.values().forEach(KafkaProducer::close);
        }
    }

    static <T> T pickRandom(List<T> list) {
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }

    static KafkaProducer<String,String> newProducer(String bootstrap, User u) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");


        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put("allow.auto.create.topics", false);

        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_34);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);

        // 🔐 유저별 인증 (SCRAM-SHA-512)
//        props.put("security.protocol", "SASL_PLAINTEXT");
//        props.put("sasl.mechanism", "SCRAM-SHA-512");
//        String password = u.password;
//        props.put("sasl.jaas.config",
//                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
//                        "username=\"" + u.name + "\" password=\"" + password + "\";");

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "OAUTHBEARER");

// ⚠️ JAAS: MDS에서 토큰 발급받기 위한 계정/비번 + MDS URL들
        String password = u.password;
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                        "username=\"" + u.name + "\" " +
                        "password=\"" + password + "\" " +
                        "metadataServerUrls=\"http://15.164.187.115:8080,http://43.200.197.192:8080,http://13.209.235.184:8080\";");

// 콜백 핸들러: 클라이언트용(User) 핸들러여야 함
        props.put("sasl.login.callback.handler.class",
                "io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler");

        // 추적 편의용
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "demo-audit-producer-" + u.name);

        return new KafkaProducer<>(props);
    }

    static String sampleJson(long loop, String who) {
        long now = System.currentTimeMillis();
        return "{"
                + "\"user\":\"" + who + "\","
                + "\"ingestTime\":" + now   + ","
                + "\"loop\":" + loop + ","
                + "\"payload\":\"demo\""
                + "}";
    }
}

