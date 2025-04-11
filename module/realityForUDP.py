import socket
import struct
import time
import random

# 定义帧起始和结束魔术字符
FRAME_START = b'\x02'  # 帧起始标识符 (STX)
FRAME_END   = b'\x03'  # 帧结束标识符 (ETX)

def send_udp_message(sock, target_ip, target_port, data, message_id=None):
    """
    UDP发送端函数：将任意长度的数据消息切分为不超过1400字节的报文段发送。
    每个报文段包含帧起始标识、报头（消息ID、总长度、分片序号、总分片数）、数据片段、帧结束标识。
    
    参数:
        sock        : 已创建的socket.socket对象 (UDP)。
        target_ip   : 目标IP地址 (字符串)。
        target_port : 目标端口号 (整数)。
        data        : 要发送的完整消息 (bytes类型或可转换为bytes的对象)。
        message_id  : 消息ID（可选整数）。如果不提供则自动生成随机ID。
    
    返回:
        使用的消息ID (整数)。
    """
    # 确保数据为bytes类型，如果是字符串则先编码
    if isinstance(data, str):
        data_bytes = data.encode('utf-8')
    else:
        data_bytes = bytes(data)
    total_length = len(data_bytes)
    
    # 生成消息ID（如未指定）
    if message_id is None:
        message_id = random.randint(1, 0xFFFFFFFF)  # 生成1到0xFFFFFFFF范围内的随机ID
    # 计算每个分片的数据负载最大长度，使单个UDP报文 <= 1400字节
    # 头部16字节 + 起始1字节 + 结束1字节 = 18字节开销
    MAX_DATAGRAM_SIZE = 1400
    HEADER_SIZE = 16  # 消息ID(4)+总长度(4)+分片序号(4)+总分片数(4)
    OVERHEAD = 1 + HEADER_SIZE + 1  # 帧起始 + 报头 + 帧结束
    max_data_per_segment = MAX_DATAGRAM_SIZE - OVERHEAD
    if max_data_per_segment <= 0:
        raise ValueError("单个分片的最大数据负载计算错误，请检查MTU设置。")
    
    # 计算总分片数量（向上取整）
    total_segments = (total_length + max_data_per_segment - 1) // max_data_per_segment
    
    # 逐个分片发送
    for seg_index in range(1, total_segments + 1):
        # 计算当前分片的数据起始和结束下标
        start = (seg_index - 1) * max_data_per_segment
        end = start + max_data_per_segment
        segment_data = data_bytes[start:end]
        
        # 构建报头 (使用网络字节序打包4个无符号32位整数)
        header_bytes = struct.pack('!IIII', message_id, total_length, seg_index, total_segments)
        
        # 构建完整的分片帧：起始标识 + 报头 + 数据 + 结束标识
        segment_packet = FRAME_START + header_bytes + segment_data + FRAME_END
        
        # 通过UDP发送此分片
        sock.sendto(segment_packet, (target_ip, target_port))
    
    # 日志：可选打印发送概况
    print(f"[发送] 消息ID={message_id}, 总长度={total_length}字节, 分片数={total_segments}")
    return message_id

def receive_udp_message(sock, assembly_timeout=5.0, duplicate_strategy='ignore'):
    """
    UDP接收端函数：接收UDP报文段并重组完整消息。
    验证帧起始/结束标识，解析报头，根据消息ID缓存分片并在全部收到后拼接。
    支持超时和重复消息处理策略。
    
    参数:
        sock              : 已绑定的socket.socket对象 (UDP) 处于接收状态。
        assembly_timeout  : 拼接超时时间（秒）。如果某消息ID的首个分片等待超过此时间仍未收齐，则丢弃该消息。
        duplicate_strategy: 重复消息处理策略，可为 'ignore' (忽略), 'reassemble' (重组), 'overwrite' (覆盖) 之一。
    
    返回:
        完整重组的消息数据 (bytes)。若在超时时间内未收到完整消息则返回 None。
    """
    # 缓存正在拼接的消息分片: {消息ID: {'src':(ip,port), 'total_length':int, 'total_segments':int,
    #                           'received':{片号:bytes}, 'start_time':time.time()}}
    messages_in_progress = {}
    # 记录已完成接收的消息ID集合，用于判断重复消息
    completed_messages = set()
    
    # 设置socket超时时间，用于跳出循环检查（并非整个拼接超时）
    sock.settimeout(0.5)
    
    while True:
        try:
            packet, addr = sock.recvfrom(2048)  # 接收一个UDP报文段（缓冲区大小设为2048字节足够容纳1400字节分片）
        except socket.timeout:
            # 定时检查超时或返回
            # 检查是否有超时未完成的消息
            current_time = time.time()
            to_remove = []
            for msg_id, info in messages_in_progress.items():
                if current_time - info['start_time'] > assembly_timeout:
                    # 拼接超时，记录需要清除的消息ID
                    print(f"[超时] 消息ID={msg_id} 超过{assembly_timeout}s仍未完成，丢弃分片数据")
                    to_remove.append(msg_id)
            # 清除超时的消息
            for msg_id in to_remove:
                messages_in_progress.pop(msg_id, None)
            # 如果缓存中有已完成的消息或者仍在拼接的消息，则继续等待其他分片直到完成或超时
            if not messages_in_progress:
                # 若当前没有未完成的消息在拼接，则跳出循环结束函数（无消息可返回）
                break
            else:
                # 仍有未完成消息，继续接收
                continue
        
        # 成功收到一个UDP分片数据包，解析处理:
        data_bytes = packet
        # 1. 验证帧起始和结束标识
        if not data_bytes or data_bytes[0:1] != FRAME_START or data_bytes[-1:] != FRAME_END:
            print("[警告] 接收到无效帧或标识符不匹配，丢弃该报文段")
            continue  # 丢弃该分片
        
        # 2. 解析报头字段
        # 报头格式: 消息ID(4字节) + 总长度(4字节) + 分片序号(4字节) + 总分片数(4字节)
        if len(data_bytes) < 1 + 16 + 1:
            # 长度不足，一个完整帧至少应有18字节
            print("[警告] 收到报文长度不足，丢弃该报文段")
            continue
        # 提取报头各字段
        header = data_bytes[1:1+16]  # 跳过帧起始标识，取接下来的16字节作为报头
        message_id, total_length, seg_index, total_segments = struct.unpack('!IIII', header)
        segment_data = data_bytes[1+16:-1]  # 去掉起始标识和报头(17字节)及末尾的结束标识，剩余即为数据
        
        # 3. 检查来源IP:Port是否一致（确保同一消息ID来自同一来源）
        if message_id in messages_in_progress:
            # 已有该消息ID在拼接中
            existing_src = messages_in_progress[message_id]['src']
            if addr != existing_src:
                # 来源不一致则丢弃
                print(f"[警告] 消息ID={message_id} 分片来源{addr}与最初{existing_src}不符，丢弃该分片")
                continue
        else:
            # 如果是新消息ID，记录来源地址
            messages_in_progress[message_id] = {
                'src': addr,
                'total_length': total_length,
                'total_segments': total_segments,
                'received': {},
                'start_time': time.time()
            }
        
        # 4. 重复消息处理策略
        if message_id in completed_messages or (message_id in messages_in_progress and len(messages_in_progress[message_id]['received']) > 0):
            # 检测到重复消息ID:
            # 条件解释: 若此ID的消息已完成 (在completed_messages中)，或者已在进行中且当前收到的这个是重复（说明之前已收到过至少一个分片，len(received)>0）。
            # 这样可以在收到第二次出现的该ID首个分片时也认为是重复消息（因为len(received)==0只有首次第一个分片）。
            if duplicate_strategy == 'ignore':
                # 忽略策略: 丢弃该ID的所有新分片
                print(f"[重复消息-忽略] 已收到消息ID={message_id}, 忽略新的分片(序号{seg_index})")
                # 对于忽略策略，我们不更新缓存，直接跳过此分片的处理
                continue
            elif duplicate_strategy == 'reassemble':
                # 重组策略: 清除旧数据，重新开始收集该ID消息
                if message_id in messages_in_progress:
                    messages_in_progress.pop(message_id, None)
                if message_id in completed_messages:
                    completed_messages.remove(message_id)
                # 重新初始化该消息的缓存结构
                messages_in_progress[message_id] = {
                    'src': addr,
                    'total_length': total_length,
                    'total_segments': total_segments,
                    'received': {},
                    'start_time': time.time()
                }
                print(f"[重复消息-重组] 检测到消息ID={message_id}重复，按照重组策略重新收集分片")
                # 注意: 重组策略下，我们继续向下执行，将当前分片当作新的开始处理（即不会continue跳过）
            elif duplicate_strategy == 'overwrite':
                # 覆盖策略: 保留已收到的分片，但用新分片内容替换相同序号的旧分片
                # （如果该消息已完成接收，则视为新的消息开始重新组装）
                if message_id in completed_messages:
                    # 如果之前已完整接收过该ID，则需要将其移出completed并重新初始化拼接（相当于新的消息实例）
                    completed_messages.remove(message_id)
                    messages_in_progress[message_id] = {
                        'src': addr,
                        'total_length': total_length,
                        'total_segments': total_segments,
                        'received': {},
                        'start_time': time.time()
                    }
                print(f"[重复消息-覆盖] 检测到消息ID={message_id}重复，按照覆盖策略更新分片数据")
                # 覆盖策略下，不清除已收到的数据，而是准备更新或添加新分片，所以不跳过处理
        
        # 5. 缓存该分片的数据
        messages_in_progress[message_id]['received'][seg_index] = segment_data
        
        # 6. 检查当前消息是否接收完所有分片
        info = messages_in_progress[message_id]
        if len(info['received']) == info['total_segments']:
            # 已收到全部分片，进行拼接
            # 按序号排序拼接数据
            sorted_segments = [info['received'][i] for i in range(1, info['total_segments'] + 1)]
            full_message = b''.join(sorted_segments)
            # 验证拼接后的总长度
            if len(full_message) == info['total_length']:
                print(f"[完成] 消息ID={message_id} 所有{info['total_segments']}个分片已拼接完成")
                # 将消息ID移出进行中列表，加入已完成集合
                messages_in_progress.pop(message_id, None)
                completed_messages.add(message_id)
                # 返回完整消息
                return full_message
            else:
                # 理论上不应发生，除非有数据缺失或重复导致长度不符
                print(f"[错误] 消息ID={message_id} 拼接后长度不匹配，应为{info['total_length']}字节，实际{len(full_message)}字节")
                # 丢弃该消息数据
                messages_in_progress.pop(message_id, None)
                completed_messages.add(message_id)
                # 未能成功拼接完整消息，继续等待下一条消息
                continue

            