package ru.orbita;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class OrbitaApplication {

  public static void main(String[] args) {
    SpringApplication.run(OrbitaApplication.class, args);
  }
}
