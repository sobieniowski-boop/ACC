"""Wrapper to run fill script and write output to file."""
import subprocess, sys
result = subprocess.run(
    [sys.executable, r"C:\ACC\tmp_fill_prices.py"],
    capture_output=True, text=True, cwd=r"C:\ACC"
)
out = result.stdout + "\n" + result.stderr
with open(r"C:\ACC\fill_output.txt", "w", encoding="utf-8") as f:
    f.write(out)
print("Done - output in C:\\ACC\\fill_output.txt")
print("Last 30 lines:")
for line in out.strip().split("\n")[-30:]:
    print(line)
