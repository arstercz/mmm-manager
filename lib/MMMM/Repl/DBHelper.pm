package MMMM::Repl::DBHelper;
use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use Carp qw(croak);
use Data::Dumper;

use constant Status => "Status";
use constant Errstr => "Errstr";

#show master status output
use constant File              => "File";
use constant Position          => "Position";
use constant Binlog_Do_DB      => "Binlog_Do_DB";
use constant Binlog_Ignore_DB  => "Binlog_Ignore_DB";
use constant Executed_Gtid_Set => "Executed_Gtid_Set";

#show slave status output
use constant Slave_IO_State              => "Slave_IO_State";
use constant Slave_SQL_Running           => "Slave_SQL_Running";
use constant Slave_IO_Running            => "Slave_IO_Running";
use constant Master_Log_File             => "Master_Log_File";
use constant Master_Host                 => "Master_Host";
use constant Master_User                 => "Master_User";
use constant Master_Port                 => "Master_Port";
use constant Replicate_Do_DB             => "Replicate_Do_DB";
use constant Replicate_Ignore_DB         => "Replicate_Ignore_DB";
use constant Replicate_Do_Table          => "Replicate_Do_Table";
use constant Replicate_Ignore_Table      => "Replicate_Ignore_Table";
use constant Replicate_Wild_Do_Table     => "Replicate_Wild_Do_Table";
use constant Replicate_Wild_Ignore_Table => "Replicate_Wild_Ignore_Table";
use constant Read_Master_Log_Pos         => "Read_Master_Log_Pos";
use constant Relay_Master_Log_File       => "Relay_Master_Log_File";
use constant Exec_Master_Log_Pos         => "Exec_Master_Log_Pos";
use constant Relay_Log_File              => "Relay_Log_File";
use constant Relay_Log_Pos               => "Relay_Log_Pos";
use constant Seconds_Behind_Master       => "Seconds_Behind_Master";
use constant Last_Errno                  => "Last_Errno";
use constant Last_Error                  => "Last_Error";
use constant Retrieved_Gtid_Set          => "Retrieved_Gtid_Set";
use constant Auto_Position               => "Auto_Position";

#general sql list
use constant Show_One_Variable_SQL  => "SHOW GLOBAL VARIABLES LIKE ?";
use constant Show_Slave_Status_SQL  => "SHOW SLAVE STATUS";
use constant Show_Processlist_SQL   => "SHOW PROCESSLIST";
use constant Show_Master_Status_SQL => "SHOW MASTER STATUS";
use constant Get_Num_Workers_SQL =>
  "SELECT \@\@global.slave_parallel_workers AS Value";
use constant Get_MaxAllowedPacket_SQL =>
  "SELECT \@\@global.max_allowed_packet AS Value";
use constant Is_Readonly_SQL  => "SELECT \@\@global.read_only As Value";
use constant Has_Gtid_SQL     => "SELECT \@\@global.gtid_mode As Value";
use constant Get_ServerID_SQL => "SELECT \@\@global.server_id As Value";
use constant Unset_Readonly_SQL       => "SET GLOBAL read_only=0";
use constant Set_Readonly_SQL         => "SET GLOBAL read_only=1";
use constant Unset_Log_Bin_Local_SQL  => "SET sql_log_bin=0";
use constant Set_Log_Bin_Local_SQL    => "SET sql_log_bin=1";
use constant Start_Slave_SQL          => "START SLAVE";
use constant Get_Version_SQL  => "SELECT VERSION() AS Value";
use constant Get_Database_SQL => "SELECT GROUP_CONCAT(SCHEMA_NAME) AS DBS FROM information_schema.SCHEMATA
WHERE SCHEMA_NAME NOT IN('mysql','test','information_schema','performance_schema')";
use constant Get_Current_Thread_Id => "SELECT CONNECTION_ID() As Value";
use constant Select_User_Regexp_SQL =>
"SELECT user, host, password FROM mysql.user WHERE user REGEXP ? AND host REGEXP ?";
use constant Set_Password_SQL             => "SET PASSWORD FOR ?\@? = ?";
use constant Old_Password_Length          => 16;
use constant Blocked_Empty_Password       => '?' x 41;
use constant Blocked_Old_Password_Head    => '~' x 25;
use constant Blocked_New_Password_Regexp  => qr/^[0-9a-fA-F]{40}\*$/o;
use constant Released_New_Password_Regexp => qr/^\*[0-9a-fA-F]{40}$/o;

sub new {
    my ( $class, %args ) = @_;
    my @required_args = qw(dbh);
    foreach my $arg (@required_args) {
        die "I need a $arg argument" unless $args{$arg};
    }
    my $self = {
        dbh           => undef,
        connection_id => undef,
        has_gtid      => undef,
        is_mariadb    => undef,
    };
    bless $self, $class;
    $self->{dbh} = $args{'dbh'};
    return $self;
}

sub get_variable {
    my $self  = shift;
    my $query = shift;
    my $sth   = $self->{dbh}->prepare($query);
    $sth->execute();
    my $href = $sth->fetchrow_hashref;
    return $href->{Value};
}

# display one value that are not supported by select @@..
sub show_variable($$) {
    my $self = shift;
    my $cond = shift;
    my $sth  = $self->{dbh}->prepare(Show_One_Variable_SQL);
    $sth->execute($cond);
    my $href = $sth->fetchrow_hashref;
    return $href->{Value};
}

sub is_binlog_enabled($) {
    my $self  = shift;
    my $value = $self->show_variable("log_bin");
    return 1 if ( defined($value) && $value eq "ON" );
    return 0;
}

sub disable_log_bin_local($) {
  my $self = shift;
  my $sth  = $self->{dbh}->prepare(Unset_Log_Bin_Local_SQL);
  return $sth->execute();
}

sub enable_log_bin_local($) {
  my $self = shift;
  my $sth  = $self->{dbh}->prepare(Set_Log_Bin_Local_SQL);
  return $sth->execute();
}

sub is_read_only($) {
    my $self = shift;
    return $self->get_variable(Is_Readonly_SQL);
}

sub enable_read_only($) {
  my $self = shift;
  my $sth  = $self->{dbh}->prepare(Set_Readonly_SQL);
  if ( $self->is_read_only() eq "1" ) {
    return 0;
  }
  else {
    return $sth->execute();
  }
}

sub disable_read_only($) {
  my $self = shift;
  my $sth  = $self->{dbh}->prepare(Unset_Readonly_SQL);
  if ( $self->is_read_only() eq "0" ) {
    return 0;
  }
  else {
    return $sth->execute();
  }
}

sub start_slave() {
  my $self = shift;
  my $sth  = $self->{dbh}->prepare(Start_Slave_SQL);
  return $sth->execute();
}

sub has_gtid($) {
    my $self  = shift;
    my $value = $self->get_variable(Has_Gtid_SQL);
    if ( defined($value) && $value eq "ON" ) {
        $self->{has_gtid} = 1;
        return 1;
    }
    return 0;
}

sub get_num_workers($) {
    my $self = shift;
    return $self->get_variable(Get_Num_Workers_SQL);
}

sub get_version($) {
    my $self  = shift;
    my $value = return $self->get_variable(Get_Version_SQL);
    if ( $value =~ /MariaDB/ ) {
        $self->{is_mariadb} = 1;
    }
    return $value;
}

sub get_server_id($) {
    my $self = shift;
    return $self->get_variable(Get_ServerID_SQL);
}

sub get_current_thread_id($) {
    my $self = shift;
    return $self->get_variable(Get_Current_Thread_Id);
}

sub get_max_allowed_packet($) {
    my $self = shift;
    return $self->get_variable(Get_MaxAllowedPacket_SQL);
}

sub get_database_list {
    my $self = shift;
    my $str  = "";
    my ($query, $sth, $href);
    $query = Get_Database_SQL;
    $sth   = $self->{dbh}->prepare($query);
    my $ret = $sth->execute();
    return if ( !defined($ret) || $ret != 1 );
    $href = $sth->fetchrow_hashref;
    if( defined $href->{DBS} && length($href->{DBS}) > 0 ) {
       $str .= sprintf("%s;", $href->{DBS});
    }
    return $str;
}

sub get_master_filter($) {
    my $self = shift;
    my ( $query, $sth, $href );
    my %values;
    my $str = "";
    $query = Show_Master_Status_SQL;
    $sth   = $self->{dbh}->prepare($query);
    my $ret = $sth->execute();
    return if ( !defined($ret) || $ret != 1 );

    $href = $sth->fetchrow_hashref;
    for my $filter_key ( Binlog_Do_DB, Binlog_Ignore_DB ) {
        if ( length( $href->{$filter_key} ) > 0 ) {
            $str .= sprintf( "%s: %s; ",
                lc($filter_key), uniq_and_sort( $href->{$filter_key} ) );
        }
    }

    return $str;
}

sub uniq_and_sort {
    my $str = shift;
    my @array = split( /,/, $str );
    my %count;
    @array = grep( !$count{$_}++, @array );
    @array = sort @array;
    return join( ',', @array );
}

sub check_slave_status {
    my $self        = shift;
    my $allow_dummy = shift;
    my ( $query, $sth, $href );
    my %status = ();

    unless ( $self->{dbh} ) {
        $status{Status} = 1;
        $status{Errstr} = "Database Handle is not defined!";
        return %status;
    }

    $query = Show_Slave_Status_SQL;
    $sth   = $self->{dbh}->prepare($query);
    my $ret = $sth->execute();
    if ( !defined($ret) || $ret != 1 ) {

        # I am not a slave
        $status{Status} = 1;

        # unexpected error
        if ( defined( $sth->errstr ) ) {
            $status{Status} = 2;
            $status{Errstr} =
                "Got error when executing "
              . Show_Slave_Status_SQL . ". "
              . $sth->errstr;
        }
        return %status;
    }

    $status{Status} = 0;
    $href = $sth->fetchrow_hashref;

    for my $key (
        Slave_IO_State,        Master_Host,
        Master_Port,           Master_User,
        Slave_IO_Running,      Slave_SQL_Running,
        Master_Log_File,       Read_Master_Log_Pos,
        Relay_Master_Log_File, Last_Errno,
        Last_Error,            Exec_Master_Log_Pos,
        Relay_Log_File,        Relay_Log_Pos,
        Seconds_Behind_Master, Retrieved_Gtid_Set,
        Executed_Gtid_Set,     Auto_Position
      )
    {
        $status{$key} = $href->{$key};
    }

    if (   !$status{Master_Host}
        || !$status{Master_Log_File} )
    {
        unless ($allow_dummy) {

            # I am not a slave
            $status{Status} = 1;
            return %status;
        }
    }

    my $str = "";
    for
      my $filter_key ( Replicate_Do_DB, Replicate_Ignore_DB, Replicate_Do_Table,
        Replicate_Ignore_Table, Replicate_Wild_Do_Table,
        Replicate_Wild_Ignore_Table )
    {
        $status{$filter_key} = uniq_and_sort( $href->{$filter_key} );
        if ( length( $href->{$filter_key} ) > 0 ) {
            $str .= sprintf( "%s: %s; ",
                lc($filter_key), uniq_and_sort( $href->{$filter_key} ) );
        }
    }
    $status{Filter} = $str;

    return %status;
}

sub get_threads_util {
    my $dbh = shift;
    my @threads;

    my $sth = $dbh->prepare(Show_Processlist_SQL);
    $sth->execute();

    while ( my $ref = $sth->fetchrow_hashref() ) {
        my $id         = $ref->{Id};
        my $user       = $ref->{User};
        my $host       = $ref->{Host};
        my $command    = $ref->{Command};
        my $state      = $ref->{State};
        my $query_time = $ref->{Time};
        my $info       = $ref->{Info};
        $info =~ s/^\s*(.*?)\s*$/$1/ if defined($info);

        push( @threads, $ref )
          if ( defined($command) && $command =~ /^Binlog Dump/ );
    }
    return @threads;
}

sub get_threads_nums($) {
    my $self = shift;
    my $excludeuser = shift;
    my @excludeusers = split(/,\s*/, $excludeuser);
    my @threads;

    my $sth = $self->{dbh}->prepare(Show_Processlist_SQL);
    $sth->execute();
    
    my $num = 0;
    my $thread_id   = $self->get_current_thread_id();
    while ( my $ref = $sth->fetchrow_hashref() ) {
        my $id      = $ref->{Id};
        my $user    = $ref->{User};
        my $db      = $ref->{db} || 'NULL';
        my $command = $ref->{Command};

        if (($id == $thread_id) || ($user eq 'event_scheduler')
            || ($user eq 'system user') || ($command =~ /^Binlog Dump/)
            || ($db eq 'information_schema') || ($db eq 'performance_schema')
            || ($db eq 'mysql') || (grep {/^$user$/} @excludeusers)) {
          next;
        }
        $num++;
    }

    return $num;
}

sub cut_host($) {
    my $info_ref = shift;
    my @hosts    = ();
    for (@$info_ref) {
        my ( $host, $sport ) = split( /:/, $_->{Host}, 2 );
        $host = '127.0.0.1' if $host =~/localhost/;
        push @hosts, $host;
    }

    return @hosts;
}

sub get_common_info($$) {
    my $self = shift;
    my %args = @_;

    my %common_hash = ();
    my %status              = $self->check_slave_status();
    my $master_port         = $status{Master_Host} . ":" . $status{Master_Port};
    $common_hash{masterhost}= $master_port;
    $common_hash{server_id} = $self->get_server_id();
    $common_hash{version}   = $self->get_version();
    $common_hash{read_only} = $self->is_read_only();
    $common_hash{delay_sec} = $status{Seconds_Behind_Master} || -1;
    $common_hash{conn_nums} = $self->get_threads_nums($args{excludeuser});
    $common_hash{databases} = $self->get_database_list();
    $common_hash{has_gtid}  = do {
        if ( $common_hash{version} =~ m/5.6/ ) {
            $self->has_gtid();
        }
        else {
            "Not Support";
        }
    };
    $common_hash{repl_check} = do {
        if ($status{Slave_SQL_Running} eq 'Yes'
            && $status{Slave_IO_Running} eq 'Yes' )
        {
            if ( $status{Seconds_Behind_Master} == 0 ) {
                "OK";
            }
            else {
                "Delay @{ [$status{Seconds_Behind_Master}] } Seconds";
            }
        }
        else {
            "Error";
        }
    };
    $common_hash{binlog_format} = $self->show_variable("binlog_format");
    $common_hash{binlog_enable} = $self->is_binlog_enabled();
    $common_hash{tx_isolation}  = $self->show_variable("tx_isolation");
    $common_hash{max_packet}    =
        $self->get_max_allowed_packet() / 1024 / 1024 . 'MB';
    $common_hash{charset}       = $self->show_variable("character_set_server");
    $common_hash{ping}          = $self->{dbh}->ping();
    $common_hash{last_check}    = time();

    return \%common_hash;

}

sub get_slave_by_master($$$$$$$$) {
    my $self       = shift;
    my $host       = shift;
    my $port       = shift;
    my $database   = shift;
    my $user       = shift;
    my $password   = shift;
    my $hosts_hash = shift;
    my $recurse    = shift;
    my %status     = ();

    my $dbpre = Repl::dbh->new(
        host     => $host,
        port     => $port,
        user     => $user,
        password => $password,
    );
    my $dbh = $dbpre->get_dbh( $database, { AutoCommit => 1 } );

    $self->{dbh} = $dbh;
    my @up_threads = Repl::DBHelper::get_threads_util($dbh);
    %status = $self->check_slave_status();
    if ( $status{Status} ) {
        $self->{dbh}                           = $dbh;
        $hosts_hash->{"$host:$port"}           = $self->get_common_info();
        $hosts_hash->{"$host:$port"}->{filter} = $self->get_master_filter();
    }

    my @slave_hosts = cut_host( \@up_threads );
    for my $slave (@slave_hosts) {
        if ($hosts_hash->{"$slave:$port"}) {
            next;
        }
        my $dbpre = Repl::dbh->new(
            host     => $slave,
            port     => $port,
            user     => $user,
            password => $password,
        );
        my $dbh_s = $dbpre->get_dbh( $database, { AutoCommit => 1 } );
        my $slave_hash;

        unless ( defined $dbh_s ) {
            $slave_hash->{"$slave:$port"}{status} = "connect error";
            push @{ $hosts_hash->{"$host:$port"}->{slave} }, $slave_hash;
            return;
        }
        $self->{dbh} = $dbh_s;
        my %status_slave = $self->check_slave_status();
        return unless defined $status_slave{Slave_SQL_Running};
        my @down_threads = Repl::DBHelper::get_threads_util($dbh_s);

        # slave has no slave.
        $slave_hash->{"$slave:$port"}               = $self->get_common_info();
        $slave_hash->{"$slave:$port"}->{filter}     = $status_slave{Filter};
        $slave_hash->{"$slave:$port"}->{repl_check} = do {
            if (   $status_slave{Slave_SQL_Running} eq 'Yes'
                && $status_slave{Slave_IO_Running} eq 'Yes' )
            {
                if ( $status_slave{Seconds_Behind_Master} < 30 ) {
                    "OK";
                }
                else {
                    "Delay @{ [$status_slave{Seconds_Behind_Master}] } Seconds";
                }
            }
            else {
                "Error";
            }
        };
        push( @{ $hosts_hash->{"$host:$port"}->{slave} }, $slave_hash );
        $self->{dbh}->disconnect();
        if ( @down_threads + 0 == 0 ) {
            next;
        }
        else {
            $self->get_slave_by_master(
                $slave, $port,
                $database,
                $user,
                $password,
                find_key_from_array(
                    $hosts_hash->{"$host:$port"},
                    "$slave:$port"
                ),
                $recurse
            ) if $recurse;
        }

    }
}

sub find_key_from_array {
    my $array_ref = shift;
    my $key       = shift;
    my $i         = 0;
    foreach ( @{ $array_ref->{slave} } ) {
        my ( $k, $v ) = each %$_;
        return $array_ref unless defined $k;
        if ( $k eq $key ) {
            return $array_ref->{slave}->[$i];
        }
        $i++;
    }
    return $array_ref;
}

sub _blocked_password {
  my $password = shift;
  if ($password eq '') {
    return Blocked_Empty_Password;
  }
  elsif (length($password) == Old_Password_Length) {
    return Blocked_Old_Password_Head . $password;
  }
  elsif ($password =~ Released_New_Password_Regexp) {
    return join("", reverse(split //, $password));
  }
  else {
    return;
  }
}

sub _released_password {
  my $password = shift;
  if ($password eq Blocked_Empty_Password) {
    return '';
  }
  elsif (index( $password, Blocked_Old_Password_Head) == 0) {
    return substr($password, length(Blocked_Old_Password_Head));
  }
  elsif ($password =~ Blocked_New_Password_Regexp) {
    return join("", reverse( split //, $password));
  }
  else {
    return;
  }
}

sub _block_release_user_by_regexp {
  my ($dbh, $user, $host, $block) = @_;
  my $users_to_block =
    $dbh->selectall_arrayref(Select_User_Regexp_SQL, {Slice => {}},
    $user, $host);
  my $failure = 0;
  for my $u (@{$users_to_block}) {
    my $password =
      $block
      ? _blocked_password($u->{password})
      : _released_password($u->{password});
    if (defined $password) {
      my $ret =
        $dbh->do(Set_Password_SQL, undef, $u->{user}, $u->{host}, $password);
      unless ($ret eq "0E0") {
        $failure++;
      }
    }
  }
  return $failure;
}

sub block_user_regexp {
  my ( $self, $user, $host ) = @_;
  return _block_release_user_by_regexp( $self->{dbh}, $user, $host, 1 );
}

sub release_user_regexp {
  my ( $self, $user, $host ) = @_;
  return _block_release_user_by_regexp( $self->{dbh}, $user, $host, 0 );
}

1;
