package com.tutorial.aws.dynamodb.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan({ "com.tutorial.aws.dynamodb" })
public class SiteMonitorApplication {

    public static void main(String[] args) {
        SpringApplication.run(SiteMonitorApplication.class, args);
    }
}