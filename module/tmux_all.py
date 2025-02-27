import os
import time

session_name = "automation"

# 1. 创建 tmux 会话
os.system(f"tmux new-session -d -s {session_name}")

# 2. 启动 test.py（但不等待它结束）
os.system(f'tmux send-keys -t {session_name} "python3 ../test/test.py" C-m')
time.sleep(2)

# 3. 创建第二个窗口
os.system(f"tmux new-window -t {session_name} -n step2")

# 4. 依次输入命令
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
    os.system(f'tmux send-keys -t {session_name}:step2 "{cmd}" C-m')
    time.sleep(1)  # 避免命令输入过快

# 5. 附加到 tmux 会话，方便观察
time.sleep(1)
os.system(f"tmux attach -t {session_name}")
#tmux kill-session -t automation