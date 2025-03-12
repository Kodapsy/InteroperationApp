import os
import time

session_name = "automation"

os.system(f"tmux new-session -d -s {session_name}")

os.system(f'tmux send-keys -t {session_name} "python3 ../test/test.py" C-m')
time.sleep(1)

os.system(f"tmux new-window -t {session_name} -n step2")
os.system(f'tmux send-keys -t {session_name}:step2 "python3 ../test/test_c.py" C-m')
time.sleep(1)

os.system(f"tmux new-window -t {session_name} -n step3")
os.system(f'tmux send-keys -t {session_name}:step3 "python3 ../test/HMI_test.py" C-m')
time.sleep(1)

os.system(f"tmux new-window -t {session_name} -n step4")

commands = [
    "scdown9",
    "sc0",
    "ame 1",
    "log all",
    "trace all",
    "psaicp printon",
    "psazmtp printon",
    "psadsmp printon",
    "msg init test_sub.usl"
]

for cmd in commands:
    os.system(f'tmux send-keys -t {session_name}:step4 "{cmd}" C-m')
    time.sleep(1)

time.sleep(1)
os.system(f"tmux attach -t {session_name}")
#tmux kill-session -t automation