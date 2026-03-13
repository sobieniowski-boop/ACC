import sys
sys.path.insert(0, "apps/api")
from app.core.security import create_access_token
token = create_access_token("smoke", "admin")
with open("_token.txt", "w") as f:
    f.write(token)
print("TOKEN WRITTEN")
