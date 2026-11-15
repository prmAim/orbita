package ru.orbita.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Set;
import org.springframework.web.filter.OncePerRequestFilter;
import ru.orbita.runtime.ApplicationReadiness;

/**
 * Applied after authorization: startup cleanup must precede every authenticated domain mutation.
 */
final class InitializationMutationFilter extends OncePerRequestFilter {
  private static final Set<String> SAFE_METHODS = Set.of("GET", "HEAD", "OPTIONS", "TRACE");
  private final ApplicationReadiness readiness;

  InitializationMutationFilter(ApplicationReadiness readiness) {
    this.readiness = readiness;
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    String path = request.getServletPath();
    if (!readiness.initialized()
        && !SAFE_METHODS.contains(request.getMethod())
        && !path.equals("/api/login")
        && !path.equals("/api/logout")) {
      response.setStatus(503);
      response.setHeader("Retry-After", "1");
      response.setContentType("application/problem+json");
      response.setCharacterEncoding("UTF-8");
      response
          .getWriter()
          .write(
              "{\"title\":\"Приложение запускается\",\"status\":503,\"detail\":\"Команды доступны после завершения восстановления состояния.\"}");
      return;
    }
    chain.doFilter(request, response);
  }
}
