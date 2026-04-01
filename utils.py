import time

def retry(func, retries=3, delay=2):
    for i in range(retries):
        try:
            return func()
        except Exception as e:
            print(f"Attempt {i+1} failed: {e}")
            time.sleep(delay)
    raise Exception(f"Failed after {retries} retries")