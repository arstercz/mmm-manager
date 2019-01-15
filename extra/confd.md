
## confd support

`mmm-identify` also set key `mysql-$uniqsign-master`, you can use [confd](https://github.com/kelseyhightower/confd) + redis
to trigger a command when the `mysql-$uniqsign-master` change.
