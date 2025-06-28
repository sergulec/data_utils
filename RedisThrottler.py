import time
class RedisThrottler:
    def __init__(self, redis_client, min_interval=1.0):
        self.redis_client = redis_client
        self.min_interval = min_interval
        self.last_update_time = {}
        

    def update(self, key, value):
        current_time = time.time()
        # Only update and publish if sufficient time has passed or the key hasn't been updated yet
        if key not in self.last_update_time or (current_time - self.last_update_time[key]) > self.min_interval:
            self.redis_client.set(key, value)  # Update the key in Redis
            self.redis_client.publish(key, value)  # Publish the update to subscribers
            self.last_update_time[key] = current_time
            #print(f"Updated and published Redis key {key} at {current_time}")
        #else:
            #print(f"Skipped update and publish for {key}, last updated at {self.last_update_time[key]}")
