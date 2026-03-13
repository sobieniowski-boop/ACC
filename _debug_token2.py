import sys, traceback
try:
    sys.path.insert(0, "apps/api")
    from app.core.security import create_access_token
    token = create_access_token("smoke", "admin")
    with open("c:\\ACC\\_token2.txt", "w") as f:
        f.write(token)
    with open("c:\\ACC\\_debug_out.txt", "w") as f:
        f.write("TOKEN_OK\n")
except Exception:
    with open("c:\\ACC\\_debug_out.txt", "w") as f:
        f.write(traceback.format_exc())
