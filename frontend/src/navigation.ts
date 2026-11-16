export const pages = ["assets", "orders", "operations", "journal", "notifications", "control", "backup"] as const;
export type Page = (typeof pages)[number];
export type OwnerRoute = {
  page: Page;
  instrument?: string;
  operation?: string;
  order?: string;
  fill?: string;
  journal?: string;
};
type Viewport = {
  scroll: number;
  focus: string | null;
};
const storageKey = "orbita-owner-navigation-v1";
export function routeHref(route: OwnerRoute): string {
  const params = new URLSearchParams();
  for (const key of ["instrument", "operation", "order", "fill", "journal"] as const)
    if (route[key] !== undefined)
      params.set(key, route[key]);
  return "#" + route.page + (params.size ? "?" + params : "");
}
export function readRoute(hash = location.hash): OwnerRoute {
  const [page, query] = hash.replace(/^#/, "").split("?");
  const route: OwnerRoute = {
    page: pages.includes(page as Page) ? page as Page : "assets"
  };
  const params = new URLSearchParams(query);
  for (const key of ["instrument", "operation", "order", "fill", "journal"] as const) {
    const value = params.get(key);
    if (value !== null)
      route[key] = value;
  }
  return route;
}
/** Identifiers remain opaque strings. Detail consumers resolve them through HTTP, never by row index. */
export class OwnerNavigation {
  route = readRoute();
  private viewports: Record<string, Viewport> = {};
  private pendingRestore = true;
  private destinationGeneration = 0;
  constructor(private changed: () => void) {
    try {
      const saved = JSON.parse(sessionStorage.getItem(storageKey) ?? "{}");
      if (saved && typeof saved === "object" && !Array.isArray(saved))
        this.viewports = saved;
    }
    catch { /* Storage may be unavailable. */
    }
    history.scrollRestoration = "manual";
    addEventListener("hashchange", () => {
      // Native fragment navigation may already have moved focus/scroll. The
      // outgoing view was saved by navigate() or the focus/scroll listeners.
      this.route = readRoute();
      this.destinationGeneration++;
      this.pendingRestore = true;
      this.changed();
    });
    addEventListener("pagehide", () => this.save());
    // Persist the current view even when a reload interrupts an in-flight request.
    addEventListener("scroll", () => this.save(), {
      passive: true
    });
    document.addEventListener("focusin", () => this.save());
  }
  navigate(route: OwnerRoute) {
    this.save();
    const href = routeHref(route);
    if (location.hash === href) {
      this.changed();
      return;
    }
    location.hash = href;
  }
  save() {
    if (this.pendingRestore)
      return;
    this.viewports[routeHref(this.route)] = {
      scroll: window.scrollY,
      focus: document.activeElement instanceof HTMLElement ? document.activeElement.id || null : null,
    };
    try {
      sessionStorage.setItem(storageKey, JSON.stringify(this.viewports));
    }
    catch { /* Navigation still works without persistence. */
    }
  }
  restore() {
    // Call after the destination has rendered, including asynchronous HTTP reads.
    if (!this.pendingRestore)
      return;
    const state = this.viewports[routeHref(this.route)];
    const href = routeHref(this.route);
    const generation = this.destinationGeneration;
    requestAnimationFrame(() => {
      if (routeHref(this.route) !== href || generation !== this.destinationGeneration)
        return;
      if (state?.focus)
        document.getElementById(state.focus)?.focus({
          preventScroll: true
        });
      window.scrollTo(0, state?.scroll ?? 0);
      this.pendingRestore = false;
    });
  }
}
