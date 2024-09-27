class SingletonLock:
    def __init__(self, redis, lock_timeout=30):
        self.redis = redis
        self.lock_key = 'app_lock'
        self.lock_timeout = lock_timeout
        self.lock_acquired = False

    def acquire_lock(self):
        is_locked = self.redis.set(self.lock_key, "LOCKED", nx=True, ex=self.lock_timeout)
        if is_locked:
            self.lock_acquired = True
            return

        raise RuntimeError("Another instance is already running.")

    def refresh_lock(self):
        if self.lock_acquired:
            self.redis.expire(self.lock_key, self.lock_timeout)

    def release_lock(self):
        if self.lock_acquired:
            self.redis.delete(self.lock_key)
            self.lock_acquired = False
            print("Lock released.")
