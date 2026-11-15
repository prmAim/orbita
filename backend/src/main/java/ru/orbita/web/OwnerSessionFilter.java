package ru.orbita.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

final class OwnerSessionFilter extends OncePerRequestFilter {
  private final OwnerCredentials credentials;

  OwnerSessionFilter(OwnerCredentials credentials) {
    this.credentials = credentials;
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    var authentication = SecurityContextHolder.getContext().getAuthentication();
    if (authentication != null
        && authentication.getPrincipal() instanceof OwnerCredentials.Owner owner
        && owner.generation() != credentials.generation()) {
      var session = request.getSession(false);
      if (session != null) session.invalidate();
      SecurityContextHolder.clearContext();
    }
    response.setHeader("Cache-Control", "no-store");
    chain.doFilter(request, response);
  }
}
