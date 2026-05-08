package com.enterprise.dataanalyst;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class DataAnalystApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataAnalystApplication.class, args);
    }
}
