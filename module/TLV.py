class TLVEncoderDecoder:
    # 定义字段映射表
    FIELD_MAP = {
        "NetProtocol": (0x0001, 4),
        "TransProtocol": (0x0002, 4),
        "NetType": (0x0003, 16),
        "PacketId": (0x0004, 8),
        "StreamId": (0x0005, 16),
        "Mid": (0x0006, 4),
        "RL": (0x0007, 4),
        "QoS": (0x0008, 4),
        "IPv4Address": (0x0009, 16),
        "IPv6Address": (0x000A, 16),
        "PortNum": (0x000B, 16),
        "ContextId": (0x000C, 16),
        "BearFlag": (0x000D, 4),
        "CommonDataType": (0x000E, 4),
        "CommonData": (0x000F, None),  # 可变长度
    }

    @staticmethod
    def encode(data: dict) -> str:
        hex_str = ""
        for key, value in data.items():
            if key not in TLVEncoderDecoder.FIELD_MAP:
                raise ValueError(f"Unknown field: {key}")

            T, L = TLVEncoderDecoder.FIELD_MAP[key]

            # 处理 ContextId 特殊情况：允许传入128位二进制字符串
            if key == "ContextId":
                if isinstance(value, str) and len(value) == 128 and all(c in '01' for c in value):
                    # 是合法的128位二进制字符串
                    value_bytes = int(value, 2).to_bytes(16, byteorder='big')
                    V_hex = value_bytes.hex()
                elif isinstance(value, bytes) and len(value) == 16:
                    V_hex = value.hex()
                else:
                    raise ValueError("ContextId must be a 128-bit binary string or 16-byte bytes")
            elif L is None:  # 可变长度字段
                V_hex = value.encode().hex()
                L = len(V_hex) // 2
            else:
                if isinstance(value, int):
                    V_hex = f"{value:0{L * 2}X}"
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
                raise ValueError(f"Unknown field T={T:04X}")

            expected_L = TLVEncoderDecoder.FIELD_MAP[key][1]

            if expected_L is None:
                # 可变长度字段，按字符串解码
                V = bytes.fromhex(V_hex).decode()
            else:
                # 固定长度，先补齐长度
                padded_hex = V_hex.zfill(expected_L * 2)  # 补足长度（以 hex 字符为单位）
                V = int(padded_hex, 16)

            data[key] = V
        return data

