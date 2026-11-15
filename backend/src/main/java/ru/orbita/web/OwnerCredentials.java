package ru.orbita.web;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OwnerCredentials implements UserDetailsService {
  private final JdbcTemplate db;
  private final PasswordEncoder encoder;

  public OwnerCredentials(JdbcTemplate db, PasswordEncoder encoder) {
    this.db = db;
    this.encoder = encoder;
  }

  @Override
  public UserDetails loadUserByUsername(String username) {
    if (!username.equals("owner")) throw new UsernameNotFoundException("Доступ закрыт");
    return db
        .query(
            "select password_hash,generation from owner_credential where singleton and password_hash is not null",
            (rs, n) -> new Owner(rs.getString(1), rs.getLong(2)))
        .stream()
        .findFirst()
        .orElseThrow(() -> new UsernameNotFoundException("Пароль не установлен"));
  }

  public long generation() {
    return db.queryForObject("select generation from owner_credential where singleton", Long.class);
  }

  @Transactional
  public void lock() {
    db.update(
        "update owner_credential set password_hash=null,generation=generation+1,changed_at=now() where singleton");
    notifyRevocation();
  }

  @Transactional
  public void replace(char[] password) {
    String value = new String(password);
    int bytes = value.getBytes(StandardCharsets.UTF_8).length;
    if (value.length() < 12 || bytes > 72)
      throw new IllegalArgumentException("Пароль: от 12 символов до 72 байт UTF-8");
    db.update(
        "update owner_credential set password_hash=?,generation=generation+1,changed_at=now() where singleton",
        encoder.encode(value));
    notifyRevocation();
  }

  private void notifyRevocation() {
    db.queryForObject(
        "select pg_notify('orbita_access_changed',?)", Object.class, Long.toString(generation()));
  }

  public record Owner(String hash, long generation) implements UserDetails {
    @Override
    public String toString() {
      return "Owner[generation=" + generation + "]";
    }

    @Override
    public String getUsername() {
      return "owner";
    }

    @Override
    public String getPassword() {
      return hash;
    }

    @Override
    public List<? extends GrantedAuthority> getAuthorities() {
      return List.of(new SimpleGrantedAuthority("ROLE_OWNER"));
    }
  }
}
