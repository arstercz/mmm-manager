## mmm-manager

MySQL multi-master manager, can be used in cloud environment, as some cloud vendors 
don't support virtual ip. can also be used with haproxy that running in backup mode.

**Currently only the active-passive is support!**

## Overview

```

                                                     3
                                                +-----------+
         +------------------------------------> | mmm-agent |
         |                                      +----^------+
         |                                           |
         V                                           |
 +----------------+                                  |
 |  +----------+  |                                  |
 |  | master A |  |               1               +-------+                2
 |  +----------+  |        +-------------+        |       |         +--------------+
 |                | -----> | mmm-monitor | -----> | Redis | <-----> | mmm-identify |
 |  +----------+  |        +-------------+        |       |         +--------------+
 |  | master B |  |                               +-------+
 |  +----------+  |
 +----------------+

```

## How does mmm-manager work?

```
1. mmm-monitor collect the masters's common status and send to Redis.

2. mmm-identify use the monitor status from Redis to identify the current active 
master, and set active/passive master keys to the Redis.

3. mmm-agent subscribe the active/passive message from the Redis, and set the 
MySQL masters, this include:

  enable/disable read_only in the active master.
  block/release user password in the active/passive master.
  enable/disable local sql log bin.
  start slave in active master when both IO and SQL thread are No.
```

## How does application connect multi-master

some MySQL client driver support [failover mode](https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-config-failover.html).
if support you can connect by failover method, such as jdbc-5.1:
```
jdbc.user.url=jdbc:mysql://10.0.21.5:3308,10.0.21.7:3308/db_test?failOverReadOnly=false
&secondsBeforeRetryMaster=60&initialTimeout=1;maxReconnects=2;autoReconnect=true
```

if don't support failover mode, you can use haproxy with backup mode:
```
# man haproxy | grep backup
        - switch to backup servers in the event a main one fails ;
```

## How to install mmm-manager

#### Dependency

mmm-manager denpend the following package:
```
perl-Redis
perl-DBI
perl-DBD-mysql
perl-Config-IniFiles
perl-Log-Dispatch
perl-Data-Dumper
perl-Authen-SASL  # if send mail by smtp server
mailx             # if send mail by mail command
```

#### INSTALL

```
make install
```

## How to set configure file

`mmm-monitor` and `mmm-identify` read the same configure file, default is 
`/etc/mmm-manager/mmm.conf`, `mmm-agent` read `/etc/mmm-manager/mmm-agent.conf`
by default.

#### mmm.conf

`mmm-monitor` and `mmm-identify` must used with `--tag` option. this means they
can work with on instance once time. you can start multiple process with different
tag to monitor multiple multi-master instance.

```
[mysql3308]                               # tag
database=information_schema
primaryhost=10.0.21.5:3308                # primary select as active master.
secondaryhost=10.0.21.7:3308
excludeuser=root, monitor, user_replica   # ignore when count mysql running thread.
user=user_mmm                             # monitor user, need privileges: SELECT, PROCESS, SUPER, REPLICATION SLAVE
password=xxxxxxxx
uniqsign=8d9a4b2924dc40baa792d742cb786345 # uniquely identifies a MySQL instance, generate by mmm-uniqsign
servicemode=1                             # 0: active-active, 1: active-passive, currently only support 1
charset=utf8
connect_timeout=1                         # timeout when connect MySQL instance
interval=3                                # sleep time as mmm-monitor and mmm-agent running in loop way
try_times=2                               # retry times when mmm-identify change the Redis key
monitor_logfile=/var/log/mmm-manager/mmm-monitor-3308.log
identify_logfile=/var/log/mmm-manager/mmm-identify-3308.log

[redis]
host=10.0.21.17:6379
topic=mmm-switch-channel                  # mmm-identify and mmm-agent use this topic to work.

[log]
logfile=/var/log/mmm-manager/mmm-monitor.log
```

#### mmm-agent.conf

`mmm-agent` subscribe the redis.topic, set mysql masters when recevies the message.
redis.topic is the uniform interface, all tags in `mmm.conf` can publish message
to this topic channel.

```
[mysql]
database=information_schema
user=user_mmm
password=xxxxxxxx
block_user=^user_app1|^user_app2$   # mmm-agent will release/block specified user@host, you can skip this if you don't set block_user and block_host options.
block_host=^10\.12\.17\.%$
charset=utf8
connect_timeout=1                   # timeout when to connect mysql
try_on_failure=3                    # retry times when connect mysql error
send_mail=1                         # whether send mail or not
email_server=10.0.21.5              # optional
email_sender=mmm_manager@mmm.com    # optional
email_receiver=arstercz@gmail.com   # necessary if send_mail

[redis]
host=10.0.21.17:6379
topic=mmm-switch-channel            # the same with mmm.conf

[log]
logfile=/var/log/mmm-manager/mmm-agent.log
```

## extra tools

`mmm-sign-gen` and `mmm-status` can be used as extra tools.

#### mmm-uniqsign

generate the uniqsign to the specified `multi-master` instance, the mmm-manager
heavily dependent on `uniqsign`:
```
# mmm-uniqsign 
747dda0fc5754935bd33f370d1bf23a6
```

#### mmm-status

`mmm-status` report the `multi-master` topology.
```
# mmm-status --conf /etc/mmm.conf 
Current multi master status:
[mysql3336]
  active: 10.0.21.5:3308(alive readonly: 0)
  +- passive: 10.0.21.7:3308(alive readonly: 1)
``` 

## TODO

```
1. support active-active mode.

2. set the etcd/consul key when set redis key. many user may want to change 
active master in hosts file immediately.

3. support MySQL 5.7 (block/relase user@host)
```

## License

MIT / BSD
