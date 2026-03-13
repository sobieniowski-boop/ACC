import subprocess, os, time, json

# Step 1: Find and kill stale backfill processes (but NOT uvicorn backend PID 5960)
print("=== CLEANUP ===")
# Use wmic to find python processes with backfill in command line
result = subprocess.run(
    ["wmic", "process", "where", "name='python.exe'", "get", "ProcessId,CommandLine", "/FORMAT:LIST"],
    capture_output=True, text=True
)
lines = result.stdout.strip().split("\n")
backfill_pids = []
for i, line in enumerate(lines):
    if "backfill" in line.lower():
        # Look for the PID line nearby
        for j in range(max(0,i-3), min(len(lines), i+3)):
            if lines[j].strip().startswith("ProcessId="):
                pid = lines[j].strip().split("=")[1]
                backfill_pids.append(pid)
                break

print(f"Found backfill PIDs: {backfill_pids}")
for pid in backfill_pids:
    try:
        subprocess.run(["taskkill", "/PID", pid, "/F"], capture_output=True)
        print(f"  Killed PID {pid}")
    except:
        pass

# Step 2: Clean up log files
base = r"C:\ACC\apps\api"
for f in ["backfill.log", "backfill_checkpoint.json", "backfill_progress.json",
          "backfill_stdout.log", "backfill_stderr.log", "backfill_launcher.log"]:
    path = os.path.join(base, f)
    if os.path.exists(path):
        try:
            os.remove(path)
            print(f"  Deleted {f}")
        except:
            print(f"  FAILED to delete {f} (locked?)")

time.sleep(2)

# Step 3: Launch via WMI (survives SSH disconnect)
print("\n=== LAUNCHING BACKFILL ===")
bat_path = r"C:\ACC\apps\api\scripts\run_backfill.bat"
wmi_cmd = f'wmic process call create "cmd /c {bat_path}"'
result = subprocess.run(wmi_cmd, capture_output=True, text=True, shell=True)
print(result.stdout)
if result.returncode == 0:
    print("Backfill launched via WMI!")
else:
    print(f"WMI launch failed: {result.stderr}")

time.sleep(3)

# Step 4: Verify it's running
result = subprocess.run(
    ["wmic", "process", "where", "name='python.exe'", "get", "ProcessId,CommandLine", "/FORMAT:LIST"],
    capture_output=True, text=True
)
if "backfill" in result.stdout.lower():
    print("CONFIRMED: Backfill process is running!")
else:
    print("WARNING: Backfill process not found in process list")
    # Check if log file was created
    log_path = os.path.join(base, "backfill.log")
    if os.path.exists(log_path):
        size = os.path.getsize(log_path)
        print(f"  But backfill.log exists ({size} bytes)")
    else:
        print("  And backfill.log does not exist yet")
