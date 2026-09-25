export class WorkGuard {
  private active = 0;

  constructor(private readonly maxConcurrent: number) {}

  get activeCount(): number {
    return this.active;
  }

  acquire(): boolean {
    if (this.active >= this.maxConcurrent) {
      return false;
    }

    this.active += 1;
    return true;
  }

  release(): void {
    this.active = Math.max(0, this.active - 1);
  }
}

export const DEFAULT_MAX_CONCURRENT_LOGIN_WORK = 100;
