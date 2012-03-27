package t::Utils;

use strict;
use warnings;
use Test::More;
use Test::mysqld;
use DBI;
use base qw/Exporter/;
our @EXPORT = qw/setup_db/;

my %_last_insert_id;

sub setup_db {
    my ($claster_name) = @_;
    my $mysqld = Test::mysqld->new(
        +{ 
            my_cnf => +{ 'skip-networking' => undef, } 
        }
    ) or plan skip_all => $Test::mysqld::errstr;

    create_schema($mysqld, $claster_name);
    insert_testdata($mysqld, $claster_name);

    return $mysqld;
}

sub last_insert_id {
    my ($table) = @_;
    $_last_insert_id{$table} = 0 unless $_last_insert_id{$table};
    ++$_last_insert_id{$table};
}

sub create_schema {
    my ($mysqld, $claster_name) = @_;

    my $SQL = <<"SQL";
        CREATE DATABASE $claster_name; 
        
        use $claster_name;
        
        CREATE TABLE table_a (
            id INTEGER UNSIGNED NOT NULL PRIMARY KEY,
            int_a   INT UNSIGNED,
            int_b   INT UNSIGNED,
            int_c   INT UNSIGNED
        ) ENGINE=InnoDB;

        CREATE TABLE table_b (
            id INTEGER UNSIGNED NOT NULL PRIMARY KEY,
            user_id INT UNSIGNED,
            int_a   INT UNSIGNED,
            int_b   INT UNSIGNED,
            int_c   INT UNSIGNED,
            text_a  VARCHAR(32),
            text_b  VARCHAR(32),
            text_c  VARCHAR(32)
        ) ENGINE=InnoDB;
SQL
    my $dbh = DBI->connect($mysqld->dsn()) or die;
    for my $sql (split /;/, $SQL) {
        next unless $sql =~ /\S/;
        $dbh->do("$sql") or die;
    }
}

sub insert_testdata {
    my ($mysqld, $claster_name) = @_;
    
    my $dbh = DBI->connect($mysqld->dsn( dbname => $claster_name )) or die;
    my $sth;

    $sth = $dbh->prepare("INSERT INTO table_a VALUES(?, ?, ?, ?)");

    for my $i ( 1 .. 100 ) {
        $sth->execute(
            last_insert_id('table_a'),
            $i,
            $i % 5 + 1,
            int(rand(20))
        );
    }

}

1;
