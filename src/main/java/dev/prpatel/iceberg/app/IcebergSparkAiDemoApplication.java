package dev.prpatel.iceberg.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.bind.annotation.RestController;


@SpringBootApplication
@RestController
@ComponentScan(basePackages = {"dev.prpatel.iceberg.app", "dev.prpatel.iceberg.tools"})
public class IcebergSparkAiDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(IcebergSparkAiDemoApplication.class, args);
    }
}
