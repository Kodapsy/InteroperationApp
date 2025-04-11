import threading
import logging
from datetime import datetime

class SessionManager:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.sessions = {}  # 存储 {context: {state_id: 状态表}}
            self.initialized = True
            self._setup_logger()

    def _setup_logger(self):
        self.logger = logging.getLogger("SessionLogger")
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler("session_log.txt", encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def log(self, message: str):
        if hasattr(self, "logger"):
            self.logger.info(message)

    @staticmethod
    def getInstance():
        if SessionManager._instance is None:
            with SessionManager._lock:
                if SessionManager._instance is None:
                    SessionManager._instance = SessionManager()
        return SessionManager._instance

    def get_or_create_state_table(self, context: str, state_id: str):
        """ 获取或创建会话的状态表 """
        instance = SessionManager.getInstance()
        with SessionManager._lock:
            if context not in instance.sessions:
                instance.sessions[context] = {}
            if state_id not in instance.sessions[context]:
                instance.sessions[context][state_id] = {}
            return instance.sessions[context][state_id]

    def get_next_state(self, mid: int, act: int = None):
        """ 仅返回下一个可能的状态，不改变当前状态 """
        state_transitions = {
            (21, 1): (22, None),
            (22, 0): (22, 2),
            (22, 1): (22, 2),
            (22, 1): (101, None),#steamSendreq
            (101, None): (102, None),#streamSendrdy
            (102, None): (106, None),#streamSendend
            (106, None): (101, None),
            (106, None): (21, 0),
            (106, None): (22, 2),
            
            (21, 0): None,
            (22, 2): None,
            (12, None): (13, None),
            (13, None): (21, 1)
        }
        key = (mid, act) if mid in [21, 22] else (mid, None)
        return state_transitions.get(key)

    def update_state(self, mid: int, context: int, act: int = None) -> bool:
        """记录当前状态，提示下一步状态，如果结束则删除会话；若遇到意外状态则报错但保留会话"""
        state_table = self.get_or_create_state_table(context, f"{mid}")
        next_state = self.get_next_state(mid, act)

        msg = f"当前状态: mid={mid}, act={act}, context={context}"
        print(msg)
        self.log(msg)

        # 定义合法结束状态组合
        valid_end_states = [(21, 0), (22, 2)]

        if next_state:
            next_msg = f"下一步可能的状态: mid={next_state[0]}, act={next_state[1]}"
            print(next_msg)
            self.log(next_msg)
        elif (mid, act) in valid_end_states:
            print("当前状态已结束，无后续状态")
            end_msg = f"会话 {context} 结束，自动删除"
            print(end_msg)
            self.log(end_msg)
            self.delete_session(context)
        else:
            error_msg = f"错误：意外的状态组合 mid={mid}, act={act}，请检查流程逻辑。会话仍保留。"
            print(error_msg)
            self.log(error_msg)
            raise ValueError(error_msg)

        return True


    def delete_session(self, context: str):
        """ 删除会话 """
        instance = SessionManager.getInstance()
        with SessionManager._lock:
            deleted = instance.sessions.pop(context, None) is not None
            if deleted:
                self.log(f"会话 {context} 已被删除")
            return deleted

    def get_active_sessions(self):
        """ 获取所有活动的会话 """
        instance = SessionManager.getInstance()
        with SessionManager._lock:
            return list(instance.sessions.keys())
