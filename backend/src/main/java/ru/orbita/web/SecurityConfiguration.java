package ru.orbita.web;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

@Configuration(proxyBeanMethods = false)
class PasswordConfiguration {
  @Bean
  PasswordEncoder passwordEncoder() {
    return new BCryptPasswordEncoder(12);
  }
}

@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
class SecurityConfiguration {
  private final AtomicInteger failures = new AtomicInteger();
  private final AtomicLong lastFailure = new AtomicLong();

  @Bean
  SecurityFilterChain security(
      HttpSecurity http,
      OwnerCredentials credentials,
      ru.orbita.runtime.ApplicationReadiness readiness)
      throws Exception {
    http.authorizeHttpRequests(
            access ->
                access
                    .requestMatchers(
                        "/",
                        "/index.html",
                        "/app.js",
                        "/navigation.js",
                        "/collection.js",
                        "/account-ui.js",
                        "/assets.js",
                        "/orders.js",
                        "/operations.js",
                        "/app.css",
                        "/api/csrf",
                        "/error")
                    .permitAll()
                    .anyRequest()
                    .hasRole("OWNER"))
        .formLogin(
            form ->
                form.loginProcessingUrl("/api/login")
                    .successHandler(
                        (req, res, auth) -> {
                          failures.set(0);
                          res.setStatus(204);
                        })
                    .failureHandler(
                        (req, res, error) -> {
                          failures.incrementAndGet();
                          lastFailure.set(Instant.now().getEpochSecond());
                          res.setStatus(401);
                        }))
        .logout(
            logout ->
                logout
                    .logoutUrl("/api/logout")
                    .invalidateHttpSession(true)
                    .deleteCookies("JSESSIONID")
                    .logoutSuccessHandler((req, res, auth) -> res.setStatus(204)))
        .exceptionHandling(
            errors -> errors.authenticationEntryPoint((req, res, error) -> res.sendError(401)))
        .requestCache(cache -> cache.disable())
        .headers(
            headers ->
                headers.contentSecurityPolicy(
                    policy ->
                        policy.policyDirectives(
                            "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")))
        .addFilterBefore(
            new OwnerSessionFilter(credentials), UsernamePasswordAuthenticationFilter.class)
        .addFilterAfter(
            new InitializationMutationFilter(readiness),
            org.springframework.security.web.access.intercept.AuthorizationFilter.class)
        .addFilterBefore(
            new org.springframework.web.filter.OncePerRequestFilter() {
              @Override
              protected void doFilterInternal(
                  jakarta.servlet.http.HttpServletRequest req,
                  jakarta.servlet.http.HttpServletResponse res,
                  jakarta.servlet.FilterChain chain)
                  throws java.io.IOException, jakarta.servlet.ServletException {
                if (req.getServletPath().equals("/api/login")
                    && req.getMethod().equals("POST")
                    && failures.get() >= 5
                    && Instant.now().getEpochSecond() - lastFailure.get() < 30) {
                  res.setHeader("Retry-After", "30");
                  res.sendError(429);
                  return;
                }
                if (Instant.now().getEpochSecond() - lastFailure.get() >= 30) failures.set(0);
                chain.doFilter(req, res);
              }
            },
            UsernamePasswordAuthenticationFilter.class);
    return http.build();
  }
}
