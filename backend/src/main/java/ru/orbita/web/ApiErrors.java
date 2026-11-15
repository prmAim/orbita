package ru.orbita.web;

import java.util.NoSuchElementException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@RestControllerAdvice
class ApiErrors extends ResponseEntityExceptionHandler {
  @ExceptionHandler(IllegalArgumentException.class)
  ProblemDetail invalid(IllegalArgumentException error) {
    return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Проверьте параметры команды");
  }

  @ExceptionHandler(IllegalStateException.class)
  ProblemDetail conflict(IllegalStateException error) {
    return ProblemDetail.forStatusAndDetail(
        HttpStatus.CONFLICT, "Действие недоступно в текущем состоянии. Обновите статус и журнал.");
  }

  @ExceptionHandler(NoSuchElementException.class)
  ProblemDetail missing(NoSuchElementException error) {
    return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, "Запись не найдена");
  }
}
