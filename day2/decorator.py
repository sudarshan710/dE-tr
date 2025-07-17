def log_time(func):
    print("I'm in log_time")
    def wrapper(*args, **kwargs):
        print(*args, **kwargs)
        print("I'm in wrapper")
        result = func(*args, **kwargs)
        return result
    return wrapper

@log_time
def process():
    print("I'm from process")

process()