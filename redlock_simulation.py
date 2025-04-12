import redis
import time
import uuid
import random
import logging
import multiprocessing
from typing import List, Tuple

client_processes_waiting: List[int] = [0, 1, 1, 1, 4]

LOG_FILE = "logger.log"
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - [%(levelname)s] - %(message)s (%(filename)s:%(lineno)d)",
    handlers=[
        logging.FileHandler(LOG_FILE)
    ]
)
# computer_id ALUNS_TEAM_1->1000 (Random value of the key)
# 1 on success

## The script that tries to release the resource in Redis
release_script_str: str = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1]) 
        else
            return 0
        end
"""

class Redlock:
    def __init__(self, redis_nodes: List[Tuple[str, int]]):
        """
        Initialize Redlock with a list of Redis node addresses.
        :param redis_nodes: List of (host, port) tuples.
        """
        # [(RedisClient, str), (RedisClient, str), (RedisClient, str), (RedisClient, str), (RedisClient, str)]
        self.redis_nodes: List[Tuple[redis.StrictRedis, str]] = []
        for node_tuple in redis_nodes:
            # utf-8
            node = redis.StrictRedis(host=node_tuple[0], port=node_tuple[1], decode_responses=True)
            script_sha = node.script_load(release_script_str)
            self.redis_nodes.append((node, script_sha))
        self.logger: logging.Logger = logging.getLogger()
        self.retry_times: int = 3
        # self.quorum: int = 3
        self.quorum: int = len(self.redis_nodes) // 2 + 1

    def acquire_lock(self, resource: str, ttl: int, client_id: int) -> Tuple[bool, str]:
        """
        Try to acquire a distributed lock.
        :param resource: The name of the resource to lock.
        :param ttl: Time-to-live for the lock in milliseconds.
        :param ttl: ID of the client that tries to acquire the key.
        :return: Tuple (lock_acquired, lock_id).
        """
        # lock_id: str = 'ALNUS_TEAM_' + str(random.randint(1, 1000)) (This was a random value for the key)
        lock_id: str = str(uuid.uuid4())
        start_time: float = time.time() * 1000
        acquired: int = 0
        redis_node_index: int = 0
        for node_tuple in self.redis_nodes:
            for i in range(self.retry_times): # 3
                try:
                    if node_tuple[0].set(resource, lock_id, nx=True, px=ttl):
                        acquired += 1
                        self.logger.info(f'{resource} was acquired on node {redis_node_index} by client {client_id} with value {lock_id}')
                        break
                    else:
                        self.logger.warning(f'Failed to acquire {resource} on node {redis_node_index} by client {client_id}')
                except Exception as e:
                    self.logger.error(f'Error acquiring lock on node {redis_node_index}: {e}')
            redis_node_index += 1
        elapsed_time: float = (time.time() * 1000) - start_time
        lock_validity: float = ttl - elapsed_time

        # 3 majority lock_id -> value with only one client because it is the lock
        if acquired >= self.quorum and lock_validity > 0:
            return True, lock_id

        self.release_lock(resource, lock_id, client_id)
        return False, None

    def release_lock(self, resource: str, lock_id: str, client_id: int) -> None:
        """
        Release the distributed lock.
        :param resource: The name of the resource to unlock.
        :param lock_id: The unique lock ID to verify ownership.
        :param client_id: The ID of the client that holds the key.
        """
        for node_tuple in self.redis_nodes:
            try:
                # (Redis, str)
                # node_tuple[0] -> redisClient
                # node_tuple[1] -> str (script) which was loaded in the __init__ function
                result: int = node_tuple[0].evalsha(node_tuple[1], 1, resource, lock_id)

                # result == 1 means it ran correctly (was released)
                if result == 1:
                    self.logger.info(f'{resource} was released on node {client_id}')
            except Exception as e:
                self.logger.error(f'Error releasing lock on node {client_id}: {e}')
                # Here, we try again
                try:
                    result: int = node_tuple[0].evalsha(node_tuple[1], 1, resource, lock_id)
                    if result == 1:
                        self.logger.info(f'{resource} was released on node {client_id} after running it again')
                    else:
                        self.logger.warning(f'{client_id} failed to release the lock')
                except Exception as e:
                    self.logger.error(f'Failed to release lock on node {client_id}: {e}')

def client_process(redis_nodes, resource: str, ttl: int, client_id: int):
    """
    Function to simulate a single client process trying to acquire and release a lock.
    :param resource: The name of the resource to unlock.
    :param ttl: Time-to-live for the lock in milliseconds.
    :param client_id: The ID of the client that holds the key.
    """
    # [0, 1, 1, 1, 4]
    time.sleep(client_processes_waiting[client_id])
    redlock = Redlock(redis_nodes)
    print(f"\nClient-{client_id}: Attempting to acquire lock...")
    lock_acquired, lock_id = redlock.acquire_lock(resource, ttl, client_id)

    if lock_acquired:
        print(f"\nClient-{client_id}: Lock acquired! Lock ID: {lock_id}")
        # Simulate critical section
        time.sleep(3)  # Simulate some work
        redlock.release_lock(resource, lock_id, client_id)
        print(f"\nClient-{client_id}: Lock released!")
    else:
        print(f"\nClient-{client_id}: Failed to acquire lock.")

if __name__ == "__main__":
    # Define Redis node addresses (host, port)
    redis_nodes = [
        ("localhost", 63791),
        ("localhost", 63792),
        ("localhost", 63793),
        ("localhost", 63794),
        ("localhost", 63795),
    ]
    with open(LOG_FILE, 'w') as logger_file:
        logger_file.write('')
    resource = "shared_resource"
    ttl = 5000  # Lock TTL in milliseconds (5 seconds)

    # Number of client processes
    num_clients = 5

    # Start multiple client processes
    processes: List[multiprocessing.Process] = []
    for i in range(num_clients):
        process = multiprocessing.Process(target=client_process, args=(redis_nodes, resource, ttl, i))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()
