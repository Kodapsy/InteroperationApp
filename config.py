selfip="192.168.20.224"
#selfip="192.168.20.223"
send_pub_port=66666
send_sub_port=55555
recv_pub_port=77777
recv_sub_port=88888

obu_sub_port=27130
obu_pub_port=27170

pub_type = 3
sub_type = 2
echo_type = 1

#数据存放目录
data_dir = "/home/nvidia/mydisk/czl/InteroperationApp/itp_data"
#data_dir = 

source_id = "A12345"
peer_id = ""
board_id = ""

echo_time = 10

appReg = 1
appAck = 2
boardCastPub = 11
boardCastSub = 12
boardCastSubNotify = 13
subScribe = 21
notify = 22
streamSendreq = 101
streamSendrdy = 102
streamRecvrdy = 103
streamSend = 104
streamRecv = 105
streamRecvend = 107
sendFile = 111
sendFin = 112
recvFile = 113

appActLogin =  1
appActLogout = 2
appActopen = 3
appActclose = 4

regack = 1
regnack = 0
delack = 3
delnack = 2
closeack = 4
closenack = 5
openack = 6
opennack = 7

pubMsg = {
    "V":0,
    "RT":int,
    "Message Type":pub_type,
    "Length":int,
    "SourceID":str,
    "DestID":None,
    "OP":int,
    "Topic":int,
    "Payload":None,
    "PayloadType":int,
    "PayloadLength":int,
    "EncodeMode":int,
}

subMsg = {
    "V":0,
    "RT":int,
    "Message Type":sub_type,
    "Length":int,
    "SourceID":str,
    "DestID":None,
    "OP":int,
    "Topic":int,
    "Payload":None,
    "PayloadType":int,
    "PayloadLength":int,
    "EncodeMode":int,
}
type_common = 2
type_data = 1
type_transD = 0
encodeBin = 0
encodeASN = 1
encodePro = 2
encodeASCII = 3