package ru.orbita.operations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestBuilders.formLogin;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import ru.orbita.notifications.NotificationPublisher;
import ru.orbita.notifications.NotificationService;
import ru.orbita.web.OwnerCredentials;

@SpringBootTest(
    properties = {
      "orbita.telegram.enabled=false",
      "orbita.backup.enabled=false",
      "orbita.worker.enabled=false"
    })
@AutoConfigureMockMvc
@Testcontainers
class OwnerAccessIT {
  @Container
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11-alpine");

  @DynamicPropertySource
  static void database(DynamicPropertyRegistry properties) {
    properties.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    properties.add("spring.datasource.username", POSTGRES::getUsername);
    properties.add("spring.datasource.password", POSTGRES::getPassword);
  }

  @Autowired MockMvc mvc;
  @Autowired OwnerCredentials credentials;
  @Autowired NotificationPublisher publisher;
  @Autowired NotificationService notifications;
  @Autowired TransactionTemplate transactions;
  @Autowired org.springframework.jdbc.core.JdbcTemplate db;

  @Test
  void positionHttpContractPreservesInventoryQualificationAndExactRatioStrings() throws Exception {
    credentials.replace("synthetic-only-passphrase".toCharArray());
    var login =
        mvc.perform(formLogin("/api/login").user("owner").password("synthetic-only-passphrase"))
            .andExpect(status().isNoContent())
            .andReturn();
    var session = (MockHttpSession) login.getRequest().getSession(false);
    db.update(
        "insert into mirror_position(instrument_id,copied_lots,manual_lots,broker_lots,broker_inventory_qualified) values('coverage-stock',2,1,3,true) on conflict(instrument_id) do update set broker_inventory_qualified=true");
    db.update(
        "update control_state set coefficient=1::numeric/3,ratio_numerator=1,ratio_denominator=3 where id=1");
    mvc.perform(get("/api/positions").session(session))
        .andExpect(status().isOk())
        .andExpect(
            jsonPath(
                "$[?(@.instrumentId=='coverage-stock')].brokerInventoryQualified",
                org.hamcrest.Matchers.hasItem(true)))
        .andExpect(
            jsonPath(
                "$[?(@.instrumentId=='coverage-stock')].brokerLots",
                org.hamcrest.Matchers.hasItem("3")));
    mvc.perform(get("/api/status").session(session))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ratioNumerator").value("1"))
        .andExpect(jsonPath("$.ratioDenominator").value("3"))
        .andExpect(jsonPath("$.coefficient").isString());
  }

  @Test
  void ownerJournalAndNotificationExposeTheSameAuthoritativeText() throws Exception {
    credentials.replace("synthetic-only-passphrase".toCharArray());
    var login =
        mvc.perform(formLogin("/api/login").user("owner").password("synthetic-only-passphrase"))
            .andExpect(status().isNoContent())
            .andReturn();
    var session = (MockHttpSession) login.getRequest().getSession(false);
    var id =
        transactions.execute(
            status ->
                publisher.publish(
                    "SKIP",
                    "SBER",
                    java.time.Instant.parse("2026-11-13T09:00:00Z"),
                    "Покупка SBER. Недостаточно собственных рублей."));
    String text = notifications.get(id).text();
    mvc.perform(get("/api/journal").session(session))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[*].detail", org.hamcrest.Matchers.hasItem(text)));
  }

  @Test
  void resetImmediatelyRevokesExistingSessionAndSignalsOpenPageBeforeNewPassword()
      throws Exception {
    credentials.replace("synthetic-only-passphrase".toCharArray());
    mvc.perform(get("/api/journal")).andExpect(status().isUnauthorized());
    var login =
        mvc.perform(formLogin("/api/login").user("owner").password("synthetic-only-passphrase"))
            .andExpect(status().isNoContent())
            .andReturn();
    var session = (MockHttpSession) login.getRequest().getSession(false);
    mvc.perform(get("/api/journal").session(session))
        .andExpect(status().isOk())
        .andExpect(header().string("Cache-Control", "no-store"));
    mvc.perform(post("/api/control/stop").session(session)).andExpect(status().isForbidden());
    var stream =
        mvc.perform(get("/api/access-events").session(session))
            .andExpect(request().asyncStarted())
            .andReturn();
    credentials.lock();
    org.awaitility.Awaitility.await()
        .atMost(java.time.Duration.ofSeconds(5))
        .untilAsserted(
            () -> assertThat(stream.getResponse().getContentAsString()).contains("access-revoked"));
    mvc.perform(get("/api/journal").session(session)).andExpect(status().isUnauthorized());
    mvc.perform(formLogin("/api/login").user("owner").password("synthetic-only-passphrase"))
        .andExpect(status().isUnauthorized());
    credentials.replace("new-synthetic-passphrase".toCharArray());
    mvc.perform(formLogin("/api/login").user("owner").password("new-synthetic-passphrase"))
        .andExpect(status().isNoContent());
  }

  @Test
  void authenticatedSessionSurvivesPageReadsAndExplicitLogoutRevokesIt() throws Exception {
    credentials.replace("synthetic-only-passphrase".toCharArray());
    var login =
        mvc.perform(formLogin("/api/login").user("owner").password("synthetic-only-passphrase"))
            .andExpect(status().isNoContent())
            .andReturn();
    var session = (MockHttpSession) login.getRequest().getSession(false);
    mvc.perform(get("/api/status").session(session)).andExpect(status().isOk());
    mvc.perform(get("/api/journal").session(session)).andExpect(status().isOk());
    mvc.perform(get("/api/status").session(session)).andExpect(status().isOk());
    mvc.perform(post("/api/logout").session(session).with(csrf()))
        .andExpect(status().isNoContent());
    mvc.perform(get("/api/journal").session(session)).andExpect(status().isUnauthorized());
  }
}
