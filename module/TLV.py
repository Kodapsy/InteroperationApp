class TLVEncoderDecoder:
    # 定义字段映射表
    FIELD_MAP = {
        "NetProtocol": (0x0001, 4),
        "TransProtocol": (0x0002, 4),
        "NetType": (0x0003, 16),
        "PacketId": (0x0004, 8),
        "StreamId": (0x0005, 16),
        "Flag": (0x0006, 4),
        "RL": (0x0007, 4),
        "QoS": (0x0008, 4),
        "IPv4Address": (0x0009, 32),
        "IPv6Address": (0x000A, 128),
        "PortNum": (0x000B, 16),
        "ContextId": (0x000C, 128),
        "BearFlag": (0x000D, 4),
        "CommonDataType": (0x000E, 4),
        "CommonData": (0x000F, None),  # 可变长度
        "Mid": (0x0010, 4),
        "Act": (0x0011, 4)
    }

    @staticmethod
    def encode(data: dict) -> str:
        hex_str = ""
        for key, value in data.items():
            if key not in TLVEncoderDecoder.FIELD_MAP:
                raise ValueError(f"Unknown field: {key}")

            T, L = TLVEncoderDecoder.FIELD_MAP[key]

            if key == "ContextId":
                # 处理ContextId为二进制字符串
                if len(value) != 128:
                    raise ValueError("ContextId should be a 128-bit binary string")
                byte_value = int(value, 2).to_bytes(16, byteorder='big')  # 128 bits = 16 bytes
                V_hex = byte_value.hex()
            elif L is None:  # 可变长度
                V_hex = value.encode().hex()
                L = len(V_hex) // 2
            else:
                if isinstance(value, int):
                    V_hex = f"{value:0{L*2}X}"
                elif isinstance(value, str):
                    V_hex = value.encode().hex()
                    if len(V_hex) // 2 != L:
                        raise ValueError(f"{key} length mismatch, expected {L}, got {len(V_hex)//2}")
                else:
                    raise ValueError(f"Unsupported data type for {key}")

            T_hex = f"{T:04X}"
            L_hex = f"{L:04X}"
            hex_str += T_hex + L_hex + V_hex

        return hex_str.upper()

    @staticmethod
    def decode(hex_str: str) -> dict:
        index = 0
        data = {}
        while index < len(hex_str):
            T = int(hex_str[index:index+4], 16)
            L = int(hex_str[index+4:index+8], 16)
            V_hex = hex_str[index+8:index+8+L*2]
            index += 8 + L*2

            key = next((k for k, v in TLVEncoderDecoder.FIELD_MAP.items() if v[0] == T), None)
            if key is None:
                raise ValueError(f"Unknown field T={T}")

            if key == "ContextId":
                # 解码为二进制字符串
                V = bin(int(V_hex, 16))[2:].zfill(L * 8)
            elif isinstance(TLVEncoderDecoder.FIELD_MAP[key][1], int):
                V = int(V_hex, 16)
            else:
                V = bytes.fromhex(V_hex).decode()

            data[key] = V
        return data

