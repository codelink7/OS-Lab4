import time

if __name__ == '__main__':
    start_time = time.time() * 1000
    time.sleep(0.1)
    elapsed_time = (time.time() * 1000) - start_time
    print(elapsed_time)
    print(type(start_time))
    print(type(elapsed_time))