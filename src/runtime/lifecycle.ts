export class LifecycleController {
  private state: "running" | "draining" | "stopped" = "running";
  private activeRequests = 0;
  private idleResolvers = new Set<() => void>();

  beginDrain(): boolean {
    if (this.state !== "running") {
      return false;
    }
    this.state = "draining";
    return true;
  }

  isReady(): boolean {
    return this.state === "running";
  }

  admitRequest(): (() => void) | undefined {
    if (this.state !== "running") {
      return undefined;
    }
    this.activeRequests += 1;
    let released = false;
    return () => {
      if (released) {
        return;
      }
      released = true;
      this.activeRequests -= 1;
      if (this.activeRequests === 0) {
        for (const resolve of this.idleResolvers) {
          resolve();
        }
        this.idleResolvers.clear();
      }
    };
  }

  activeRequestCount(): number {
    return this.activeRequests;
  }

  waitForIdle(): Promise<void> {
    if (this.activeRequests === 0) {
      return Promise.resolve();
    }
    return new Promise((resolve) => this.idleResolvers.add(resolve));
  }

  markStopped(): void {
    this.state = "stopped";
  }
}
