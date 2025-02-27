class AbilityManager:
    _instance = None
    _abilities = {}

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(AbilityManager, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def update_abilities(self, role, abilities):
        """更新角色的能力"""
        self._abilities[role] = set(abilities)

    def query_abilities(self):
        """查询所有角色的能力"""
        return self._abilities