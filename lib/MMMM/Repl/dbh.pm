package MMMM::Repl::dbh;

# Get the database handle which user use, and this database
# handle object should be destroy when leave MySQL database.
use strict;
use warnings FATAL => 'all';
use constant PTDEBUG => $ENV{PTDEBUG} || 0;
use English qw(-no_match_vars);
use DBI;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

use Data::Dumper;
$Data::Dumper::Indent    = 1;
$Data::Dumper::Sortkeys  = 1;
$Data::Dumper::Quotekeys = 0;

require Exporter;
@ISA     = qw(Exporter);
@EXPORT  = qw( get_dbh disconnect );
$VERSION = '0.1.0';

eval { require DBI; };

if ($@) {
    die "Cannot connect to MySQL because the Perl DBI module is not "
      . "installed or not found.  Run 'perl -MDBI' to see the directories "
      . "that Perl searches for DBI.  If DBI is not installed, try:\n"
      . "  Debian/Ubuntu  apt-get install libdbi-perl\n"
      . "  RHEL/CentOS    yum install perl-DBI\n"
      . "  OpenSolaris    pkg install pkg:/SUNWpmdbi\n";
}

sub host {
    my $self = shift;
    $self->{host} = shift if @_;
    return $self->{host};
}

sub port {
    my $self = shift;
    $self->{port} = shift if @_;
    return $self->{port};
}

sub user {
    my $self = shift;
    $self->{user} = shift if @_;
    return $self->{user};
}

sub password {
    my $self = shift;
    $self->{password} = shift if @_;
    return $self->{password};
}

sub charset {
    my $self = shift;
    $self->{charset} = shift if @_;
    return $self->{charset};
}

sub driver {
    my $self = shift;
    $self->{driver} = shift if @_;
    return $self->{driver};
}

sub timeout {
    my $self = shift;
    $self->{timeout} = shift if @_;
    return $self->{timeout};
}

sub new {
    my ( $class, %args ) = @_;
    my @required_args = qw(host port user password);
    PTDEBUG && print Dumper(%args);

    foreach my $arg (@required_args) {
        warn "I need a $arg argument" unless $args{$arg};
    }

    my $self = {};
    bless $self, $class;

    # options should be used.
    $self->host( $args{'host'}         || 127.0.0.1 );
    $self->port( $args{'port'}         || 3306 );
    $self->user( $args{'user'}         || 'audit' );
    $self->password( $args{'password'} || '' );
    $self->charset( $args{'charset'}   || 'utf8' );
    $self->driver( $args{'driver'}     || 'mysql' );
    $self->timeout( $args{'timeout'}   || 1 );

    return $self;
}

sub get_dbh {
    my ( $self, $database, $opts ) = @_;
    $opts ||= {};
    my $host     = $self->{host};
    my $port     = $self->{port};
    my $user     = $self->{user};
    my $password = $self->{password};
    my $charset  = $self->{charset};
    my $driver   = $self->{driver};
    my $timeout  = $self->{timeout};

    my $defaults = {
        AutoCommit         => 0,
        RaiseError         => 1,
        PrintError         => 0,
        ShowErrorStatement => 1,
        mysql_enable_utf8  => ( $charset =~ m/utf8/i ? 1 : 0 ),
    };
    @{$defaults}{ keys %$opts } = values %$opts;

    #if ( $opts->{mysql_use_result} ) {
    #    $defaults->{mysql_use_result} = 1;
    #}

    my $dbh;
    my $tries = 2;
    while ( !$dbh && $tries-- ) {
        PTDEBUG
          && print Dumper(
            join( ', ', map { "$_=>$defaults->{$_}" } keys %$defaults ) );
        $dbh = eval {
            DBI->connect(
                "DBI:$driver:mysql_connect_timeout=$timeout:database=$database;host=$host;port=$port",
                $user, $password, $defaults );
        };

        if ( !$dbh && $@ ) {
            if ( $@ =~ m/locate DBD\/mysql/i ) {
                warn
"Cannot connect to MySQL because the Perl DBD::mysql module is "
                  . "not installed or not found.  Run 'perl -MDBD::mysql' to see "
                  . "the directories that Perl searches for DBD::mysql.  If "
                  . "DBD::mysql is not installed, try:\n"
                  . "  Debian/Ubuntu  apt-get install libdbd-mysql-perl\n"
                  . "  RHEL/CentOS    yum install perl-DBD-MySQL\n"
                  . "  OpenSolaris    pgk install pkg:/SUNWapu13dbd-mysql\n";
            }
            elsif ( $@ =~ m/not a compiled character set|character set utf8/i )
            {
                PTDEBUG && print 'Going to try again without utf8 support\n';
                delete $defaults->{mysql_enable_utf8};
            }
            if ( !$tries ) {
                warn "$@";
                return;
            }

        }
    }

    if ( $driver =~ m/mysql/i ) {
        my $sql;
        $sql = 'SELECT @@SQL_MODE';
        PTDEBUG && print "+-- $sql\n";

        my ($sql_mode) = eval { $dbh->selectrow_array($sql) };
        warn "Error getting the current SQL_MORE: $@" if $@;

        if ($charset) {
            $sql = qq{/*!40101 SET NAMES "$charset"*/};
            PTDEBUG && print "+-- $sql\n";
            eval { $dbh->do($sql) };
            warn "Error setting NAMES to $charset: $@" if $@;
            PTDEBUG && print "Enabling charset to STDOUT\n";
            if ( $charset eq 'utf8' ) {
                binmode( STDOUT, ':utf8' )
                  or warn "Can't binmode(STDOUT, ':utf8'): $!\n";
            }
            else {
                binmode(STDOUT) or warn "Can't binmode(STDOUT): $!\n";
            }
        }

        $sql =
            'SET @@SQL_QUOTE_SHOW_CREATE = 1'
          . '/*!40101, @@SQL_MODE=\'NO_AUTO_VALUE_ON_ZERO'
          . ( $sql_mode ? ",$sql_mode" : '' ) . '\'*/';
        PTDEBUG && print "+-- $sql\n";
        eval { $dbh->do($sql) };
        warn "Error setting SQL_QUOTE_SHOW_CREATE, SQL_MODE"
          . ( $sql_mode ? " and $sql_mode" : '' ) . ": $@"
          if $@;
    }

    if (PTDEBUG) {
        print Dumper(
            $dbh->selectrow_hashref(
'SELECT DATABASE(), CONNECTION_ID(), VERSION()/*!50038, @@hostname*/'
            )
        );
        print "+-- 'Connection info:', $dbh->{mysql_hostinfo}\n";
        print Dumper(
            $dbh->selectall_arrayref(
                "SHOW VARIABLES LIKE 'character_set%'",
                { Slice => {} }
            )
        );
        print '+-- $DBD::mysql::VERSION:' . "$DBD::mysql::VERSION\n";
        print '+-- $DBI::VERSION:' . "$DBI::VERSION\n";
    }
    return $dbh;
}

# handle should be destroy.
sub disconnect {
    my ( $self, $dbh ) = @_;
    PTDEBUG && $self->print_active_handles( $self->get_dbh );
    $dbh->disconnect;
}

sub print_active_handles {
    my ( $self, $thing, $level ) = @_;
    $level ||= 0;
    printf(
        "# Active %sh: %s %s %s\n",
        ( $thing->{Type} || 'undef' ),
        "\t" x $level,
        $thing,
        ( ( $thing->{Type} || '' ) eq 'st' ? $thing->{Statement} || '' : '' )
    ) or warn "Cannot print: $OS_ERROR";
    foreach my $handle ( grep { defined } @{ $thing->{ChildHandles} } ) {
        $self->print_active_handles( $handle, $level + 1 );
    }
}

1;
