import subprocess, sys
r = subprocess.run([sys.executable, r"C:\ACC\tmp_fill_v2.py"], capture_output=True, text=True, timeout=180, cwd=r"C:\ACC")
with open(r"C:\ACC\fill_v2_out.txt", "w") as f:
    f.write("STDOUT:\n" + r.stdout + "\nSTDERR:\n" + r.stderr + f"\nRC={r.returncode}\n")
# Print last 40 lines only
lines = (r.stdout + "\n" + r.stderr).strip().split("\n")
for l in lines[-40:]:
    print(l)
