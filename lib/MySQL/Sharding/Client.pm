package MySQL::Sharding::Client;

use strict;
use warnings;

use DBI;
use MySQL::Sharding::Client::ResultSet;
use Carp qw/croak/;

our $VERSION = '0.0.1';

sub connect {
    my ($class, %args) = @_;
    my $self = bless {
        connect_infos => $args{connect_infos},
        user          => $args{user},
        password      => $args{password},
        dbhs          => {},
        tables        => {},
    }, $class;

    foreach my $name (keys %{$self->{connect_infos}}) {
        $self->_connect($name, %{$self->{connect_infos}{$name}});
    }
    return $self;
}

sub disconnect {
    my ($self) = @_;
    $self->{dbhs}{$_}->disconnect foreach (keys %{$self->{dbhs}});
}

sub dbh {
  my ($self, $name) = @_;
  return $self->{dbhs}{$name};
}

sub ping {
    my ($self, $name) = @_;
    unless ($name) {
        for $name ( keys %{$self->{dbhs}} ) {
            return 0 unless $self->dbh($name)->ping;
        }
    } else {
        return 0 unless $self->dbh($name)->ping;
    }
    return 1;
}

sub prepare {
    my ($self, $sql) = @_;

    $sql = $self->_clean_sql($sql);
    $sql = $self->_replace_alias($sql);

    my $statment = $self->parse_sql($sql);

    if ($statment->{offset} && $statment->{offset} > 0) {
        croak "ERROR not support OFFSET. SQL=$sql";
    }

    if ($statment->{group}) {
        for my $param (@{$statment->{group}}) {
            unless (grep {$_->{name} eq $param} @{ $statment->{columns} }) {
                croak "ERROR not found grouped column in field list";
            }
        }
    }

    my $result_set = MySQL::Sharding::Client::ResultSet->new( %$statment );

    foreach my $name (keys %{$self->{dbhs}}) {
        my $stmt = $self->dbh($name)->prepare($sql);
        $result_set->add_stmt($stmt, $name);
    }

    return $result_set;
}

sub do {
    my ($self, $sql) = @_;
    
    my $result_set = $self->prepare($sql);

    $result_set->execute();

    return $result_set;
}

sub parse_sql {
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

sub _connect {
    my ($self, $name, %config) = @_;

    return if ($self->{dbhs}{$name} && $self->{dbhs}{$name}->ping);

    my $dbh = DBI->connect(
        $config{dsn},
        exists $config{user} ? $config{user} : $self->{user},
        exists $config{password} ? $config{password} : $self->{password},
        $config{options}
    );

    my $sth = $dbh->prepare("show tables");
    $sth->execute();
    my @tables;
    while (my ($table) = $sth->fetchrow_array) {
        push @tables, $table;
    }

    $self->{dbhs}{$name}   = $dbh;
    $self->{tables}{$name} = \@tables;
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
    foreach my $col_str ( split ",", $columns_str ) {
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
    foreach my $column (split / *, */, $1) {
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
    foreach my $column (split / *, */, $1) {
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
__END__

=head1 NAME

MySQL::Sharding::Client - Perl extention to do can be use as one DBI handle to many DBI handles.

=head1 VERSION

This document describes MySQL::Sharding::Client version 0.0.1.

=head1 SYNOPSIS

    use MySQL::Sharding::Client;

    # connect any dsn
    my $dbh = MySQL::Sharding::Client->connect(
        connect_infos => {
            shard01 => {
                dsn      => $shard01_dsn, 
                user     => $shard01_user, 
                password => $shard01_password, 
                options  => $shard01_dboption,
            },
            shard02 => {
                dsn => $shard02_dsn,
            },
        }, 
        user          => $default_user,
        password      => $default_pass,
    );

    # ping to all dbhs.
    die "connect fail" unless $dbh->ping;

    # do statement
    # usable SQL command is only `SELECT` or `SET`.
    my $sth = $dbh->do( $statement );

    # prepare and execute
    $sth = $dbh->prepare( $statement );

    $sth->execute;
    $sth->execute( @bind_value );

    $rv = $sth->rows;

    @row_ary  = $sth->fetchrow_array;
    %row_hash = $sth->fetchrow_hash;
    $ary_ref  = $sth->fetchrow_arrayref;
    $hash_ref = $sth->fetchrow_hashref;

    $sth->disconnect;

=head1 DESCRIPTION

This module is to do can be use as one DBI handle to many DBI handles.
DML is supported in this module is only SELECT.
And support some analyzed command COUNT, SUM, MAX, MIN.

=head1 DEPENDENCIES

Perl 5.8.1 or later.

=head1 BUGS

All complex software has bugs lurking in it, and this module is no
exception. If you find a bug please either email me, or add the bug
to cpan-RT.

=head1 SEE ALSO

L<perl>

=head1 AUTHOR

Kensaku Araga E<lt>ken39arg {at} gmail.com<gt>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2012, Kensaku Araga. All rights reserved.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
