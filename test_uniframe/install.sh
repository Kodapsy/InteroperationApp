if [ -z "$UNIDIR" ]; then
   echo "Please install UniFrame and set \$UNIDIR first"
   exit 1
fi
if [ -f $UNIDIR/etc/sc.env ]; then
   DEFAULT_BASEPORT=`grep -w uniBasePort $UNIDIR/etc/sc.env | awk '{print $2}'`
fi
printf "base port ("$DEFAULT_BASEPORT"): "
read BASEPORT
if [ -z "$BASEPORT" ]; then
   BASEPORT=$DEFAULT_BASEPORT
fi
if [ -z "$BASEPORT" ]; then
   echo "Please input a base port"
   exit 1
fi

uniinstall_copyfile.sh uni $UNIDIR

useetc test

echo uniBasePort $BASEPORT > $UNIDIR/etc/sc.env

echo
HOSTNAME=`hostname`
MYIPADDR=`cat /etc/hosts | sed 's/#.*//g' | grep -w $HOSTNAME | grep -v 127.0.0.1 | head -1 | awk '{print $1}'`
printf "local ip address? ($MYIPADDR) "
read LOCALIP
if [ -z "$LOCALIP" ]; then LOCALIP=$MYIPADDR; fi

LOCALPORTDSMP=`expr $BASEPORT + 200`
LOCALPORTICP=`expr $BASEPORT + 201`
LOCALPORTITP=`expr $BASEPORT + 202`

printf "obu ip address? ($MYIPADDR) "
read REMOTEIP
if [ -z "$REMOTEIP" ]; then REMOTEIP=$MYIPADDR; fi

DEFAULTREMOTEPORT=`expr $BASEPORT + 300`
printf "obu udp port? ($DEFAULTREMOTEPORT) "
read REMOTEPORT
if [ -z "$REMOTEPORT" ]; then REMOTEPORT=$DEFAULTREMOTEPORT; fi

DEFAULTICPAID=0
printf "icp aid? ($DEFAULTICPAID) "
read ICPAID
if [ -z "$ICPAID" ]; then ICPAID=$DEFAULTICPAID; fi

DEFAULTITPAID=1
printf "itp aid? ($DEFAULTITPAID) "
read ITPAID
if [ -z "$ITPAID" ]; then ITPAID=$DEFAULTITPAID; fi

# 新增 DeviceId 的处理逻辑
DEFAULTDEVICEID="11"  # 默认 DeviceId 值
printf "DeviceId? ($DEFAULTDEVICEID): "
read DEVICEID
if [ -z "$DEVICEID" ]; then
   DEVICEID=$DEFAULTDEVICEID
fi

replace -s\
	_LOCAL_IP_ $LOCALIP\
	_LOCAL_PORT_DSMP_ $LOCALPORTDSMP\
	_LOCAL_PORT_ICP_ $LOCALPORTICP\
	_LOCAL_PORT_ITP_ $LOCALPORTITP\
	_REMOTE_IP_ $REMOTEIP\
	_REMOTE_PORT_ $REMOTEPORT\
	_ICP_AID_ $ICPAID\
	_ITP_AID_ $ITPAID\
	_DEVICE_ID_ $DEVICEID \
	-- $UNIDIR/etc/psaicp.cfg $UNIDIR/etc/psaitp.cfg $UNIDIR/etc/psadsmp.cfg $UNIDIR/etc/psazmtp.cfg.v1

echo
echo install finished
echo
