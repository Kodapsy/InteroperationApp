class TLVEncoderDecoder:
    # 定义字段映射表
    FIELD_MAP = {
    "NetProtocol":     {"tag": 0x0001, "length": 4,  "type": "int"},
    "TransProtocol":   {"tag": 0x0002, "length": 4,  "type": "int"},
    "NetType":         {"tag": 0x0003, "length": 16, "type": "str"},
    "PacketId":        {"tag": 0x0004, "length": 8,  "type": "int"},
    "StreamId":        {"tag": 0x0005, "length": 16, "type": "hex"},
    "Mid":             {"tag": 0x0006, "length": 4,  "type": "int"},
    "RL":              {"tag": 0x0007, "length": 4,  "type": "int"},
    "QoS":             {"tag": 0x0008, "length": 4,  "type": "int"},
    "IPv4Address":     {"tag": 0x0009, "length": 16, "type": "str"},
    "IPv6Address":     {"tag": 0x000A, "length": 16, "type": "str"},
    "PortNum":         {"tag": 0x000B, "length": 16, "type": "int"},
    "ContextId":       {"tag": 0x000C, "length": 16, "type": "hex"},
    "BearFlag":        {"tag": 0x000D, "length": 4,  "type": "int"},
    "CommonDataType":  {"tag": 0x000E, "length": 4,  "type": "int"},
    "CommonData":      {"tag": 0x000F, "length": None, "type": "hex"},
}
    # 定义字段类型映射表

    @staticmethod
    def encode(data: dict) -> str:
        hex_str = ""
        for key, value in data.items():
            if key not in TLVEncoderDecoder.FIELD_MAP:
                raise ValueError(f"Unknown field: {key}")
            
            field = TLVEncoderDecoder.FIELD_MAP[key]
            T = field["tag"]
            L = field["length"]
            dtype = field["type"]

            if dtype == "int":
                V_hex = f"{value:0{L * 2}X}"
            elif dtype == "str":
                V_hex = value.encode().hex()
                if L is not None and len(V_hex) // 2 != L:
                    raise ValueError(f"{key} length mismatch, expected {L}, got {len(V_hex)//2}")
            elif dtype == "hex":
                if not isinstance(value, str) or len(value) != L * 2 or any(c not in "0123456789abcdefABCDEF" for c in value):
                    raise ValueError(f"{key} must be a {L * 2}-char valid hex string")
                V_hex = value.lower()
            else:
                raise ValueError(f"Unsupported data type '{dtype}' for key '{key}'")

            
            actual_L = L if L is not None else len(V_hex) // 2
            T_hex = f"{T:04X}"
            L_hex = f"{actual_L:04X}"
            hex_str += T_hex + L_hex + V_hex

        return hex_str.upper()


    @staticmethod
    def decode(hex_str: str) -> dict:
        index = 0
        data = {}

        # 反向映射 TAG → key
        tag_to_key = {v["tag"]: k for k, v in TLVEncoderDecoder.FIELD_MAP.items()}

        while index < len(hex_str):
            T = int(hex_str[index:index+4], 16)
            L = int(hex_str[index+4:index+8], 16)
            V_hex = hex_str[index+8:index+8+L*2]
            index += 8 + L*2

            key = tag_to_key.get(T)
            if key is None:
                raise ValueError(f"Unknown tag: {T:04X}")

            field = TLVEncoderDecoder.FIELD_MAP[key]
            dtype = field["type"]

            if dtype == "int":
                V = int(V_hex, 16)
            elif dtype == "hex":
                V = V_hex.lower()
            elif dtype == "str":
                V = bytes.fromhex(V_hex).decode()
            else:
                raise ValueError(f"Unsupported decode type: {dtype}")

            data[key] = V

        return data


