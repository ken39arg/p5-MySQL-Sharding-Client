use strict;
use warnings;

use Test::More;
use DBIx::Sharding::Client;
use Data::Dumper;

test {

    subtest 'require' => sub {
        require_ok 'DBIx::Sharding::Client';
    };

    subtest 'method' => sub {
        can_ok 'DBIx::Sharding::Client', (
            'new',
            'connect',
            'do',
        );
    };

    subtest 'new' => sub {
    };

    subtest 'connect' => sub {
    };

    subtest 'do' => sub {
    };

    subtest '_parse_sql' => sub {
        my $parsed;

        $parsed = DBIx::Sharding::Client->_parse_sql("select a, b, c from t_table where a = '123' and b > 5  order  by  c limit 10 offset 5");
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

        $parsed = DBIx::Sharding::Client->_parse_sql(<<"SQL");
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

        $parsed = DBIx::Sharding::Client->_parse_sql(<<"SQL");
            DESC user
SQL
        is_deeply $parsed, {
            command => "SHOW",
            type    => "columns",
        }, "desc SQL";
        $parsed = DBIx::Sharding::Client->_parse_sql(<<"SQL");
            SET NAMES utf8 
SQL
        is_deeply $parsed, {
            command => "SET",
            type    => "character_set_name",
        }, "set names utf8 SQL";
        $parsed = DBIx::Sharding::Client->_parse_sql(<<"SQL");
            set  sql_big_selects = 1;
SQL
        is_deeply $parsed, {
            command => "SET",
            type    => "variable_assignment",
        }, "set sql_big_selects SQL";
    };
};
