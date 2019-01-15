package MMMM::Monitor::Redis;

use strict;
use warnings;
use English qw(-no_match_vars);
use Log::Dispatch qw(add log);
use Redis;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

use constant PTDEBUG => $ENV{PTDEBUG} || 0;
use Data::Dumper;
$Data::Dumper::Indent    = 1;
$Data::Dumper::Sortkeys  = 1;
$Data::Dumper::Quotekeys = 0;

require Exporter;
@ISA = qw(Exporter);
@EXPORT    = qw( common_set mode_set status_set );
$VERSION = '0.1.0';

sub handle {
  my $self = shift;
  my %args = @_;

  $self->{handle} = Redis->new(%args);
  return $self->{handle};
}

sub new {
    my ($class, %args) = @_;
    PTDEBUG && _debug(%args);

    if ($args{sentinels}) {
      $args{sentinels_cnx_timeout}   ||= 1;
      $args{sentinels_read_timeout}  ||= 1;
      $args{sentinels_write_timeout} ||= 1;
    } else {
      $args{reconnect}     ||= 2;
      $args{every}         ||= 100_000;
      $args{cnx_timeout}   ||= 1;
      $args{read_timeout}  ||= 1;
      $args{write_timeout} ||= 1;
    }
    
    my $self = {};
    bless $self, $class;
    $self->handle(%args);

    PTDEBUG && _debug($self->{handle});
    return $self;
}

sub common_set {
  my $self = shift;
  my %args = @_;

  my @masters  = @{$args{masters}};
  my $uniqsign = $args{uniqsign};
  my $log      = $args{log};
  my $servicemode  = $args{servicemode};
  my $primaryhost  = $args{primaryhost};
  my $secondaryhost= $args{secondaryhost};

  # set basic message and sadd mysql repl members
  eval {
    $self->{handle}->setnx("mysql-$uniqsign-servicemode",
                           $servicemode);
    $self->{handle}->sadd("mysql-$uniqsign-members",
                           @masters);

    $self->{handle}->setnx("mysql-$uniqsign-primaryhost",
                           $primaryhost);

    $self->{handle}->setnx("mysql-$uniqsign-secondaryhost",
                           $secondaryhost);
  };
  if ($@) {
    $log->error("master_check error: $@");
  }
  else {
    $log->info("master_check ok - " 
               . join(", ", @masters)
               . ", uniqsign: $uniqsign");
  }
}

sub service_mode {
  my $self = shift;
  my $uniqsign = shift;

  return $self->{handle}->get("mysql-$uniqsign-servicemode");
}

sub is_members_ok {
  my $self = shift;
  my %args = @_;
  my $uniqsign = $args{uniqsign};
  my $log      = $args{log};
  my %masters_info  = map { $_ => 1 }  @{$args{masters}};
  my @get_masters;

  eval {
    @get_masters  = $self->{handle}->smembers("mysql-$uniqsign-members");
  };
  if ($@) {
    $log->error("cann't get members: $@");
  }

  my $status = 1;
  foreach my $k (@get_masters) {
    unless (defined $masters_info{$k}) {
      $status = 0;
    }
  }

  return $status;
}

sub status_set {
  my $self = shift;
  my %args = @_;
  my $log  = $args{log};
  my $uniqsign   = $args{uniqsign};

  my %status = ();
  foreach my $host (@{$args{masters}}) {
    foreach my $k (qw(ping repl_check read_only
             delay_sec conn_nums last_check )) {
      $status{$host}{$k} = $args{$host}{$k};
    }
  }

  foreach my $k (keys %status) {
    my $key = "mysql-$uniqsign-$k-check";
    eval {
      $self->{handle}->hmset($key, %{$status{$k}});
    };
    if ($@) {
      $log->error("set $key error: $@");
    }
    else {
      $log->info("set $key ok.");
    }
  }
}

sub status_get {
  my $self = shift;
  my $host = shift;
  my %args = @_;
  my $log  = $args{log};
  my $uniqsign   = $args{uniqsign};

  my $status;
  my $key = "mysql-$uniqsign-$host-check";

  foreach my $k (qw(ping repl_check read_only
         delay_sec conn_nums last_check )) {
    my $v;
    eval { $v = shift $self->{handle}->hmget($key, $k); };
    if ($@) {
      $log->error("hmget $host $key check error: $@");
    }
    else {
      return undef unless defined $v; 
      $status->{$k} = $v;
      #$log->info("hmget $host $key check ok.");
    }
  }
  return $status;
}

sub mode_set {
  my $self = shift;
  my %args = @_;
  my $uniqsign = $args{uniqsign};
  my $log      = $args{log};
  my $activehost = $args{current_master};
  my $passivehost;

  if (defined $activehost) {
    ($passivehost) = grep {!/$activehost/} @{$args{masters}};
    $log->info("mode_set: get active: $activehost and "
               . "passive: $passivehost.");
  }
  else {
    $log->error("mode_set: cann't get active and passive host");
    return undef;
  }

  unless ($self->is_mode_change($log, $uniqsign, $activehost)) {
    $log->info("mode_set current_master $activehost equal the " 
             . "redis key mysql-$uniqsign-master, skip set master...");
    return undef;
  }

  eval {
    $self->{handle}->hmset("mysql-$uniqsign-mode", 
           ("active", $activehost, "passive", $passivehost));
    $self->{handle}->hmset("mysql-$uniqsign-master", 
           ("host", $activehost));
  };
  if ($@) {
    $log->error("set mysql-{mode,master} error: $@");
  }
  else {
    $log->info("set mysql-{mode,master} ok.");
  }

  # send message to channel with uniform interface
  my $redis_topic = $args{topic};
  my $message     = 
     "active: $activehost, passive: $passivehost, uniqsign: $uniqsign";
  eval {
    $self->{handle}->publish($redis_topic, $message);
  };
  if ($@) {
    $log->error("publish $redis_topic $message error: $@");
  }
  else {
    $log->info("publish $redis_topic $message ok.");
  }
}

sub is_mode_change {
  my $self = shift;
  my $log  = shift;
  my $uniqsign   = shift;
  my $activehost = shift;

  # check wether update key value
  my $master_res;
  eval {
    $master_res = 
       $self->{handle}->hmget("mysql-$uniqsign-master", "host");
  };
  if ($@) {
    $log->error("mode_set: cann't get mysql-$uniqsign-master value: $@");
  }

  if ((defined $master_res->[0]) and (defined $activehost)
       and ($master_res->[0] eq $activehost)) {
    return 0;
  }

  return 1;
}

1;
