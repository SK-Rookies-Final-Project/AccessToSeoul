package com.seoul.login;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class LoginToSeoul {
    public static void main(String[] args) {
        String bootstrap = "15.164.187.115:9092,43.200.197.192:9092,13.209.235.184:9092";
        String user = "wrong";
        String password = "wrong-secret";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-512");

        String jaasCfg = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                user, password
        );
        props.put("sasl.jaas.config", jaasCfg);

        // 재시도 횟수를 랜덤(1~5)으로 결정
        Random random = new Random();
        int retries = 1 + random.nextInt(5);

        System.out.printf("[LOGIN] %s will attempt SCRAM auth %d times within 5 minutes%n", user, retries);

        for (int i = 1; i <= retries; i++) {
            try (AdminClient admin = AdminClient.create(props)) {
                admin.describeCluster().nodes().get(); // SCRAM 인증 시도
                System.out.printf("[LOGIN-SUCCESS] %s authenticated via SCRAM (attempt %d)%n", user, i);
            } catch (Exception e) {
                System.err.printf("[LOGIN-FAIL] %s SCRAM auth error (attempt %d): %s%n", user, i, e.getMessage());
            }

            if (i < retries) {
                // 5분(300초) 내 랜덤 간격으로 sleep
                int sleepSeconds = 10 + random.nextInt(290); // 10 ~ 300초 사이
                try {
                    System.out.printf("[WAIT] Sleeping %d seconds before next attempt%n", sleepSeconds);
                    TimeUnit.SECONDS.sleep(sleepSeconds);
                } catch (InterruptedException ignored) {}
            }
        }

        System.out.printf("[DONE] %s finished SCRAM attempts%n", user);
    }
}