authStatus = False

def auth(func):
    def wrapper(*args, **kwargs):
        global authStatus
        if (not authStatus):
            print('\n Access denied. Please log in.')
        else:
            func(*args, **kwargs)
    return wrapper

@auth
def dashboard():
    print('\n Welcome to my dashboard!')

@auth
def logout():
    global authStatus
    authStatus = False

def login():
    global authStatus
    authStatus = True

dashboard()
login()
dashboard()
logout()
dashboard()