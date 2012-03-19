package DBIx::Sharding::Client;

use strict;
use warnings;

use DBI;
use Carp qw/croak/;
use Data::Dumper;

sub new {
    my ($class, %args) = @_;
    my $self = bless {
        connect_infos => $args{connect_infos},
        user          => $args{user},
        password      => $args{password},
        dbhs          => [],
    }, $class;

    if ($self->{connect_infos}) {
        $self->connect;
    }
}

sub connect {
    my ($self) = @_;

    $self->_connect(%$_) for ( @{ $self->{connect_infos} } );
}

sub do {
    my ($self, $sql) = @_;
}

sub select {
    my ($self, $sql) = @_;
}

sub show {
    my ($self, $sql) = @_;
}

sub _connect {
    my ($self, %config) = @_;

    push @{ $self->{dbhs} }, DBI->connect( 
        $config{dsn},
        exists $config{user} ? $config{user} : $self->{user},
        exists $config{password} ? $config{password} : $self->{password},
        $config{options}
    );
}

sub _parse_sql {
    my ($self, $sql) = @_;

    $sql = $self->_clean_sql($sql);
    $sql = $self->_replace_alias($sql);

    my %statment;

    $statment{command} = $self->_parse_command($sql);

    if ($statment{command} eq 'SELECT') {
        $statment{columns} = $self->_parse_columns($sql);
        $statment{group}   = $self->_parse_group($sql);
        $statment{order}   = $self->_parse_order($sql);
        $statment{limit}   = $self->_parse_limit($sql);
        $statment{offset}  = $self->_parse_offset($sql);
    } elsif ($statment{command} eq 'SHOW') {
        $statment{type} = $self->_parse_show($sql);
    } elsif ($statment{command} eq 'SET') {
        $statment{type} = $self->_parse_set($sql);
    } else {
        croak "ERROR not support '".$statment{command}."'. SQL=$sql";
    }

    \%statment;
}

sub _clean_sql {
    my ($self, $sql) = @_;

    chomp $sql;

    $sql =~ s/[\n\r]+/ /g;
    $sql =~ s/\t/ /g;
    $sql =~ s/^ +//g;
    $sql =~ s/\s+/ /g;

    return $sql;
}

sub _replace_alias {
    my ($self, $sql) = @_;
    $sql =~ s/^(DESCRIBE|DESC)/SHOW COLUMNS FROM/i;

    return $sql;
}

sub _parse_command {
    my ($self, $sql) = @_;
    unless ($sql =~ m/^(\w+)/i ) {
        croak "ERROR can't parse this SQL '$sql'";
    }
    uc $1;
}

sub _parse_columns {
    my ($self, $sql) = @_;
    unless ($sql =~ m/^SELECT +(.+?) +FROM +([^\s]+)/i) {
        croak "ERROR can't find fields. '$sql'";
    }

    my $columns_str = $1;
    my @columns;
    for my $col_str ( split ",", $columns_str ) {
        my %col;
        $col_str =~ s/^\s*(.*?)\s*$/$1/;
        if ($col_str =~ m/(.+) +AS +(.+)/i) {
            $col{column}  = $1;
            $col{name} = $2;
        } else {
            $col{column}  = $col_str;
            $col{name} = $col_str;
        }

        if ($col{column} =~ m/^(\w+)\(.+\)/i) {
            $col{command} = uc $1;
        } else {
            $col{command} = 'NONE';
        }
        push @columns, \%col;
    }

    \@columns;
}

sub _parse_group {
    my ($self, $sql) = @_;
    unless ($sql =~ m/GROUP +BY +(.+?) *(HAVING|UNION|ORDER|LIMIT|$)/i) {
        return undef;
    }

    my @group;
    for my $column (split / *, */, $1) {
        push @group, $column;
    }
    \@group;
}

sub _parse_order {
    my ($self, $sql) = @_;
    unless ($sql =~ m/ORDER +BY +(.+?) *(LIMIT|$)/i) {
        return undef;
    }

    my @order;
    for my $column (split / *, */, $1) {
        my @order_val = split /\s/, $column;
        push @order, {
            column => $order_val[0],
            order  => 1 < @order_val ? uc $order_val[1] : 'ASC',
        };
    }
    \@order;
}

sub _parse_limit {
    my ($self, $sql) = @_;
    if ($sql =~ m/LIMIT +(\d+)/i) {
        return int $1;
    }
    return 0;
}

sub _parse_offset {
    my ($self, $sql) = @_;
    if ($sql =~ m/offset +(\d+)/i) {
        return int $1;
    }
    return 0;
}

sub _parse_show {
    my ($self, $sql) = @_;
    if ($sql =~ m/^SHOW +TABLE +STATUS *(.*)/i) {
        return 'table_status';
    } elsif ($sql =~ m/^SHOW( +OPEN +| +)TABLES *(.*)/i) {
        return 'tables';
    } elsif ($sql =~ m/^SHOW( +FULL +| +)COLUMNS FROM *(.*)/i) {
        return 'columns';
    } else {
        croak "Unsupport SHOW statement. $sql";
    }
}

sub _parse_set {
    my ($self, $sql) = @_;
    if ($sql =~ m/^SET +NAMES +(.+)/i || $sql =~ m/^SET +CHARACTER +SET +(.+)/i) {
        return "character_set_name";
    } elsif ($sql =~ m/^SET +(.+)$/i) {
        return 'variable_assignment';
    } else {
        croak "Parse error SET statement.";
    }
}

1;
