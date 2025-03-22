import redis
import time
import uuid
import logging
import multiprocessing
from typing import List, Tuple

client_processes_waiting = [0, 1, 1, 1, 4]

LOG_FILE = "logger.log"
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - [%(levelname)s] - %(message)s (%(filename)s:%(lineno)d)",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

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
        self.redis_nodes: List[Tuple[redis.StrictRedis, str]] = []
        for node_tuple in redis_nodes:
            node = redis.StrictRedis(host=node_tuple[0], port=node_tuple[1], decode_responses=True)
            script_sha = node.script_load(release_script_str)
            self.redis_nodes.append((node, script_sha))
        self.logger: logging.Logger = logging.getLogger()
        self.retry_times: int = 5
        self.quorum: int = len(self.redis_nodes) // 2 + 1
        
    def acquire_lock(self, resource: str, ttl: int) -> Tuple[bool, str]:
        """
        Try to acquire a distributed lock.
        :param resource: The name of the resource to lock.
        :param ttl: Time-to-live for the lock in milliseconds.
        :return: Tuple (lock_acquired, lock_id).
        """
        lock_id: str = str(uuid.uuid4())
        start_time: float = time.time() * 1000
        acquired: int = 0

        for node_tuple in self.redis_nodes:
            for i in range(self.retry_times):
                if node_tuple[0].set(resource, lock_id, nx=True, px=ttl):
                    acquired += 1
                    self.logger.info(f'{lock_id} was acquired {acquired} times')
                    break
                else:
                    self.logger.warning(f'Failed to acquire {lock_id}')
        elapsed_time: float = (time.time() * 1000) - start_time
        lock_validity: float = ttl - elapsed_time

        if acquired >= self.quorum and lock_validity > 0:
            return True, lock_id

        self.release_lock(resource, lock_id)
        return False, None


    def release_lock(self, resource: str, lock_id: str) -> None:
        """
        Release the distributed lock.
        :param resource: The name of the resource to unlock.
        :param lock_id: The unique lock ID to verify ownership.
        """
        for node_tuple in self.redis_nodes:
            try:
                node_tuple[0].evalsha(node_tuple[1], resource)
                self.logger.info(f'{lock_id} was released')
            except Exception as e:
                self.logger.warning(f'{lock_id} wasn\'t released successfully')


def client_process(redis_nodes, resource: str, ttl: int, client_id: int):
    """
    Function to simulate a single client process trying to acquire and release a lock.
    """
    time.sleep(client_processes_waiting[client_id])

    redlock = Redlock(redis_nodes)
    print(f"\nClient-{client_id}: Attempting to acquire lock...")
    lock_acquired, lock_id = redlock.acquire_lock(resource, ttl)

    if lock_acquired:
        print(f"\nClient-{client_id}: Lock acquired! Lock ID: {lock_id}")
        # Simulate critical section
        time.sleep(3)  # Simulate some work
        redlock.release_lock(resource, lock_id)
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

    resource = "shared_resource"
    ttl = 5000  # Lock TTL in milliseconds (5 seconds)

    # Number of client processes
    num_clients = 5

    # Start multiple client processes
    processes = []
    for i in range(num_clients):
        process = multiprocessing.Process(target=client_process, args=(redis_nodes, resource, ttl, i))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()
