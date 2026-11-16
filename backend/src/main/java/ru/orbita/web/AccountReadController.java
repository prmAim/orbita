package ru.orbita.web;

import java.util.Map;
import org.springframework.web.bind.annotation.*;
import ru.orbita.account.AccountQuery;
import ru.orbita.account.AccountReadService;

@RestController
@RequestMapping("/api/account")
final class AccountReadController {
  private final AccountReadService reads;

  AccountReadController(AccountReadService reads) {
    this.reads = reads;
  }

  @GetMapping("/assets")
  AccountReadService.Page assets(@RequestParam Map<String, String> query) {
    return reads.page("assets", AccountQuery.parse(query));
  }

  @GetMapping("/orders")
  AccountReadService.Page orders(@RequestParam Map<String, String> query) {
    return reads.page("orders", AccountQuery.parse(query));
  }

  @GetMapping("/operations")
  AccountReadService.Page operations(@RequestParam Map<String, String> query) {
    return reads.page("operations", AccountQuery.parse(query));
  }

  @GetMapping("/order")
  Map<String, Object> order(@RequestParam String id) {
    return reads.exact("ORDER", id);
  }

  @GetMapping("/operation")
  Map<String, Object> operation(@RequestParam String id) {
    return reads.exact("OPERATION", id);
  }

  @GetMapping("/fill")
  Map<String, Object> fill(@RequestParam String id) {
    return reads.exact("FILL", id);
  }

  @GetMapping("/journal")
  Map<String, Object> journal(@RequestParam long id) {
    return reads.journal(id);
  }
}
