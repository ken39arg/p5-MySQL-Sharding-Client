use strict;
use warnings;

use t::Utils;
use Test::More;

BEGIN {
    use_ok 'MySQL::Sharding::Client';
    use_ok 'MySQL::Sharding::Client::ResultSet';
    use_ok 'MySQL::Sharding::Client::Prompt';

    require_ok 'MySQL::Sharding::Client';
    require_ok 'MySQL::Sharding::Client::ResultSet';
    require_ok 'MySQL::Sharding::Client::Prompt';
    
    can_ok 'MySQL::Sharding::Client', (
        'connect',
        'disconnect',
        'parse_sql',
        'prepare',
        'do',
        'ping',
    );
    can_ok 'MySQL::Sharding::Client::ResultSet', (
        'new',
        'add_stmt',
        'execute',
        'fetchrow',
        'fetchrow_array',
        'fetchrow_hash',
        'fetchrow_arrayref',
        'fetchrow_hashref',
        'rows',
        'has_next_row',
    );
    can_ok 'MySQL::Sharding::Client::Prompt', (
        'new',
        'run',
    );
}

note "Start set up Test::mysqld.";

my $connect_infos = {};
my $user     = "root";
my $password = "";

my %mysqld;

foreach my $name ('db1', 'db2', 'db3') {
    $mysqld{$name} = setup_db($name);
    $connect_infos->{$name} = {
        dsn => $mysqld{$name}->dsn( dbname => $name ),
    };
}

note "Done set up Test::mysqld.";

{
    local $@;
    my $dbhandler;
    eval {
        $dbhandler = MySQL::Sharding::Client->connect(
            connect_infos => $connect_infos, 
            user          => $user,
            password      => $password,
        );
    };
    if ($@) {
        fail("connect $@");
    } else {
        pass("connect");
    }

    ok $dbhandler->ping, "ping";

    subtest 'check all table' => sub { 
        foreach my $name ('db1', 'db2', 'db3') {
            my $dbh = $dbhandler->dbh($name);
            my $sth;

            $sth = $dbh->prepare("show tables like 'table_a'");
            $sth->execute();
            is $sth->fetchrow_arrayref->[0], 'table_a', "has table $name";

            $sth = $dbh->prepare("select count(*) from table_a");
            $sth->execute();
            ok 0 < $sth->fetchrow_arrayref->[0], "count table $name";
        }
    };

    eval {
        $dbhandler->disconnect();
    };
    if ($@) {
        fail("disconnect $@");
    } else {
        pass("disconnect");
    }

    subtest 'pre commands' => sub { 
        eval {
            $dbhandler = MySQL::Sharding::Client->connect(
                connect_infos => $connect_infos, 
                user          => $user,
                password      => $password,
                pre_commands  => [
                    'set SQL_MAX_JOIN_SIZE=1028',
                    'insert into table_b set id = 999, user_id = 123',
                ],
            );
        };
        if ($@) {
            fail("connect $@");
        } else {
            pass("connect with pre_commands");
        }
        foreach my $name ('db1', 'db2', 'db3') {
            my $dbh = $dbhandler->dbh($name);
            my $sth;

            $sth = $dbh->prepare("select * from table_b where id = ?");
            $sth->execute(999);
            ok $sth->rows, "prepare insert OK $name";
            is $sth->fetchrow_hashref->{user_id}, '123', "set val $name";

            $sth = $dbh->prepare("show variables like 'max_join_size'");
            $sth->execute();
            is $sth->fetchrow_arrayref->[1], 1028, "count table $name";
        }
        $dbhandler->disconnect();
    };

}

subtest 'parse_sql' => sub {
    my $parsed;

    $parsed = MySQL::Sharding::Client->parse_sql("select a, b, c from t_table where a = '123' and b > 5  order  by  c limit 10 offset 5");
    is_deeply $parsed, {
        command => "SELECT",
        columns => [
            {column => 'a', name => 'a', command => 'NONE'},
            {column => 'b', name => 'b', command => 'NONE'},
            {column => 'c', name => 'c', command => 'NONE'},
        ],
        group   => undef,
        order   => [{column => 'c', order => 'ASC'}],
        limit   => 10,
        offset  => 5,
    }, "parse simple SQL. use lower camel.";

    $parsed = MySQL::Sharding::Client->parse_sql(<<"SQL");
        SELECT 
            f_abc, 
            sum(f_bcd), 
            count(distinct f_ccb) as uu 
        FROM t_table 
        WHERE f_bcd > 5 
        GROUP BY f_abc 
        ORDER  BY  c DESC
        LIMIT 100 
        OFFSET 60 
SQL
    is_deeply $parsed, {
        command => "SELECT",
        columns => [
            {column => 'f_abc',                 name => 'f_abc'     , command => 'NONE'},
            {column => 'sum(f_bcd)',            name => 'sum(f_bcd)', command => 'SUM'},
            {column => 'count(distinct f_ccb)', name => 'uu'        , command => 'COUNT'},
        ],
        group   => ['f_abc'],
        order   => [{column => 'c', order => 'DESC'}],
        limit   => 100,
        offset  => 60,
    }, "parse some SQL. use upper camel.";

    $parsed = MySQL::Sharding::Client->parse_sql("select ts, count(u) as uu from (select u, (sum(point) + 49) DIV 50 as ts from tbl group by u) m group by ts");
    is_deeply $parsed, {
        command => "SELECT",
        columns => [
            {column => 'ts', name => 'ts', command => 'NONE'},
            {column => 'count(u)', name => 'uu', command => 'COUNT'},
        ],
        group   => ['ts'],
        order   => undef,
        limit   => 0,
        offset  => 0,
    }, "parse sub query.";

    $parsed = MySQL::Sharding::Client->parse_sql(<<"SQL");
        select
            t.t_id,
            fday,
            count(uid) as user_num,
            datediff(end, fday) + 1 as full_day,
            SUM(case when datediff(end, fday) + 1 = num then 1 else 0 end) as full,
            SUM(case when num = 1 then 1 else 0 end) as once,
            sum(num) as total
        from (
            select
                t_id,
                uid,
                min(day)   as fday,
                max(day)   as lday,
                count(day) as num
            from t_test 
            group by t_id, uid
        ) t
        left join (
            select
                t_id,
                max(day)   as end
            from t_test
            group by t_id
        ) s
        ON t.t_id = s.t_id
        group by t.t_id, fday;
SQL
    is_deeply $parsed, {
        command => "SELECT",
        columns => [
            {column => 't.t_id', name => 't.t_id', command => 'NONE'},
            {column => 'fday', name => 'fday', command => 'NONE'},
            {column => 'count(uid)', name => 'user_num', command => 'COUNT'},
            {column => 'datediff(end, fday) + 1', name => 'full_day', command => 'NONE'},
            {column => 'SUM(case when datediff(end, fday) + 1 = num then 1 else 0 end)', name => 'full', command => 'SUM'},
            {column => 'SUM(case when num = 1 then 1 else 0 end)', name => 'once', command => 'SUM'},
            {column => 'sum(num)', name => 'total', command => 'SUM'},
        ],
        group   => ['t.t_id', 'fday'],
        order   => undef,
        limit   => 0,
        offset  => 0,
    }, "parse sub query with another func.";

    $parsed = MySQL::Sharding::Client->parse_sql(<<"SQL");
        DESC user
SQL
    is_deeply $parsed, {
        command => "SHOW",
        type    => "columns",
    }, "desc SQL";
    $parsed = MySQL::Sharding::Client->parse_sql(<<"SQL");
        SET NAMES utf8 
SQL
    is_deeply $parsed, {
        command => "SET",
        type    => "character_set_name",
    }, "set names utf8 SQL";
    $parsed = MySQL::Sharding::Client->parse_sql(<<"SQL");
        set  sql_big_selects = 1;
SQL
    is_deeply $parsed, {
        command => "SET",
        type    => "variable_assignment",
    }, "set sql_big_selects SQL";
};

{
    my $dbhandler = MySQL::Sharding::Client->connect(
        connect_infos => $connect_infos, 
        user          => $user,
        password      => $password,
    );

    subtest "count all" => sub {
        my $sql = "SELECT COUNT(*) FROM table_a";
        my $rs = $dbhandler->do($sql);

        is $rs->rows, 1, "rows";
        is $rs->fetchrow_arrayref->[0], 300, "result";
    };

    subtest "sum all" => sub {
        my $sql = "SELECT SUM(id) FROM table_a";
        my $rs = $dbhandler->do($sql);

        is $rs->rows, 1, "rows";

        my $r = 0;
        $r += $_ for (1 .. 300);

        is $rs->fetchrow_arrayref->[0], $r, "result";
    };

    subtest "max all" => sub {
        my $sql = "SELECT MAX(id) as a, MAX(int_a) as b FROM table_a";
        my $rs = $dbhandler->do($sql);

        is $rs->rows, 1, "rows";
        my %row = $rs->fetchrow_hash;
        is $row{a}, 300, "result";
        is $row{b}, 100, "result";
    };

    subtest "min all" => sub {
        my $sql = "SELECT MIN(int_a) FROM table_a";
        my $rs = $dbhandler->do($sql);

        is $rs->rows, 1, "rows";
        is $rs->fetchrow_arrayref->[0], 1, "result";
    };

    subtest "group by and count (use fetchrow_array)" => sub {
        my $sql = "SELECT int_b, count(*) from table_a group by int_b";
        my %dbi_res;
        foreach my $name ('db1', 'db2', 'db3') {
            my $sth = $dbhandler->dbh($name)->prepare($sql);
            $sth->execute();
            while (my ($k, $v) = $sth->fetchrow_array) {
                $dbi_res{$k} += $v;
            }
        }
        my $rs = $dbhandler->do($sql);

        is scalar(keys %dbi_res), $rs->rows, "rows";

        while (my ($k, $v) = $rs->fetchrow_array) {
            is $v, $dbi_res{$k},  "result row $k = $v";
        }
    };

    subtest "group by 2 colmuns and any functions (use fetchrow_hashref)" => sub {
        my $sql = <<"SQL";
            SELECT 
                int_b, 
                int_c, 
                count(*) AS con, 
                sum(int_a) AS sm, 
                max(int_a) AS mx, 
                min(int_a) AS mn 
            FROM table_a 
            WHERE int_b > 3 
            GROUP BY int_b, int_c
SQL
        my %dbi_res;
        foreach my $name ('db1', 'db2', 'db3') {
            my $sth = $dbhandler->dbh($name)->prepare($sql);
            $sth->execute();
            while (my $row = $sth->fetchrow_hashref) {
                my $key = $row->{int_b} . "-" .$row->{int_c};
                if ($dbi_res{$key}) {
                    $dbi_res{$key}{con} += $row->{con};
                    $dbi_res{$key}{sm} += $row->{sm};
                    $dbi_res{$key}{mx} = $row->{mx} if $dbi_res{$key}{mx} < $row->{mx};
                    $dbi_res{$key}{mn} = $row->{mn} if $row->{mn} < $dbi_res{$key}{mn};
                } else {
                    $dbi_res{$key} = $row;
                }
            }
        }
        my $rs = $dbhandler->do($sql);

        is scalar(keys %dbi_res), $rs->rows, "rows";

        my ($ok_con, $ok_sm, $ok_mx, $ok_mn) = (1,1,1,1);
        while (my $row = $rs->fetchrow_hashref) {
            my $key = $row->{int_b} . "-" .$row->{int_c};
            $ok_con = 0 if $row->{con} != $dbi_res{$key}->{con};
            $ok_sm  = 0 if $row->{sm}  != $dbi_res{$key}->{sm};
            $ok_mx  = 0 if $row->{mx}  != $dbi_res{$key}->{mx};
            $ok_mn  = 0 if $row->{mn}  != $dbi_res{$key}->{mn};
        }

        ok $ok_con, "all rows count match.";
        ok $ok_sm,  "all rows sum match.";
        ok $ok_mx,  "all rows max match.";
        ok $ok_mn,  "all rows min match.";
    };

    subtest "select and order asc" => sub {
        my $sql = "SELECT id, int_a, int_b FROM table_a WHERE int_b > ? AND int_c < ? ORDER BY int_a ASC";
        my @dbi_res;
        foreach my $name ('db1', 'db2', 'db3') {
            my $sth = $dbhandler->dbh($name)->prepare($sql);
            $sth->execute(3, 7);
            while (my $row = $sth->fetchrow_hashref) {
                push @dbi_res, $row;
            }
        }
        @dbi_res = sort {$a->{int_a} <=> $b->{int_a}} @dbi_res;
        my $rs = $dbhandler->prepare($sql);
        $rs->execute(3, 7);

        is $rs->rows, scalar @dbi_res, "rows";
        
        my $i  = 0;
        my $ok = 1;
        while (my $row = $rs->fetchrow_hashref) {
            $ok = 0 unless ($row->{id} == $dbi_res[$i]->{id});
            $ok = 0 unless ($row->{int_a} == $dbi_res[$i]->{int_a});
            $ok = 0 unless ($row->{int_b} == $dbi_res[$i]->{int_b});
            unless ($ok) {
                diag "# not match row";
                diag "## got";
                diag explain $row;
                diag "## expected";
                diag explain $dbi_res[$i];
                last;
            }
            ++$i;
        }

        ok $ok, "all rows check.";
    };

    subtest "select and order desc and limit" => sub {
        my $sql = "SELECT id, int_a, int_b FROM table_a WHERE int_b > ? AND int_c < ? ORDER BY int_a DESC LIMIT 5";
        my @dbi_res;
        foreach my $name ('db1', 'db2', 'db3') {
            my $sth = $dbhandler->dbh($name)->prepare($sql);
            $sth->execute(3, 15);
            while (my $row = $sth->fetchrow_hashref) {
                push @dbi_res, $row;
            }
        }
        @dbi_res = sort {$b->{int_a} <=> $a->{int_a}} @dbi_res;
        my $rs = $dbhandler->prepare($sql);
        $rs->execute(3, 15);

        is $rs->rows, 5, "rows";
        
        my $i  = 0;
        my $ok = 1;
        while (my $row = $rs->fetchrow_hashref) {
            $ok = 0 unless ($row->{id} == $dbi_res[$i]->{id});
            $ok = 0 unless ($row->{int_a} == $dbi_res[$i]->{int_a});
            $ok = 0 unless ($row->{int_b} == $dbi_res[$i]->{int_b});

            unless ($ok) {
                diag "# not match row";
                diag "## got";
                diag explain $row;
                diag "## expected";
                diag explain $dbi_res[$i];
                last;
            }
            ++$i;
        }

        ok $ok, "all rows check.";
    };

    subtest "select and multi order and limit" => sub {
        my $sql = "SELECT id, int_a, int_b FROM table_a WHERE int_b > ? AND int_c < ? ORDER BY int_b, int_a DESC LIMIT 30";
        my @dbi_res;
        foreach my $name ('db1', 'db2', 'db3') {
            my $sth = $dbhandler->dbh($name)->prepare($sql);
            $sth->execute(2, 10);
            while (my $row = $sth->fetchrow_hashref) {
                push @dbi_res, $row;
            }
        }
        @dbi_res = sort {$a->{int_b} <=> $b->{int_b} || $b->{int_a} <=> $a->{int_a}} @dbi_res;
        my $rs = $dbhandler->prepare($sql);
        $rs->execute(2, 10);

        is $rs->rows, 30, "rows";
        
        my $i  = 0;
        my $ok = 1;
        while (my $row = $rs->fetchrow_hashref) {
            $ok = 0 unless ($row->{id} == $dbi_res[$i]->{id});
            $ok = 0 unless ($row->{int_a} == $dbi_res[$i]->{int_a});
            $ok = 0 unless ($row->{int_b} == $dbi_res[$i]->{int_b});

            unless ($ok) {
                diag "# not match row";
                diag "## got";
                diag explain $row;
                diag "## expected";
                diag explain $dbi_res[$i];
                last;
            }
            ++$i;
        }

        ok $ok, "all rows check.";
    };

};


done_testing;

