package ru.orbita.operations;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@SpringBootTest(
    properties = {
      "orbita.worker.enabled=false",
      "orbita.telegram.enabled=false",
      "orbita.backup.enabled=false"
    })
@Testcontainers
class BackupLifecycleIT {
  @Container
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine");

  @DynamicPropertySource
  static void database(DynamicPropertyRegistry properties) {
    properties.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    properties.add("spring.datasource.username", POSTGRES::getUsername);
    properties.add("spring.datasource.password", POSTGRES::getPassword);
  }

  @Autowired JdbcTemplate db;
  @Autowired Environment environment;
  @TempDir Path directory;

  @Test
  void restoredMonthlyMetadataNeverOverwritesAnExistingUsbVersion() throws Exception {
    Path local = Files.createDirectory(directory.resolve("local"));
    Path usb = Files.createDirectory(directory.resolve("usb"));
    Path archive = usb.resolve("orbita-2026-11.dump");
    Path checksum = usb.resolve("orbita-2026-11.dump.sha256");
    Files.writeString(archive, "existing verified monthly bytes");
    Files.writeString(checksum, "existing verified checksum");
    var service =
        new BackupService(
            new BackupSettings(
                true,
                local,
                usb,
                POSTGRES.getDatabaseName(),
                POSTGRES.getHost(),
                POSTGRES.getMappedPort(5432),
                POSTGRES.getUsername()),
            db,
            Clock.fixed(Instant.parse("2026-11-15T10:00:00Z"), ZoneOffset.UTC),
            environment);
    service.tick();
    service.tick();
    assertThat(service.list())
        .singleElement()
        .satisfies(version -> assertThat(version.status()).isEqualTo("EXISTING_COPY_UNCONFIRMED"));
    assertThat(Files.readString(archive)).isEqualTo("existing verified monthly bytes");
    assertThat(Files.readString(checksum)).isEqualTo("existing verified checksum");
    assertThat(local.resolve("orbita-2026-11.dump")).doesNotExist();
  }
}
