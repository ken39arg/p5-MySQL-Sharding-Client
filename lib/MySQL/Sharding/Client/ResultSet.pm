package MySQL::Sharding::Client::ResultSet;

use strict;
use warnings;

use Time::HiRes qw/gettimeofday tv_interval/;
use Carp qw/croak carp/;

use constant AGGREGATE_FUNCTIONS => qw/SUM COUNT MAX MIN/;

sub new {
    my ($class, %args) = @_;
    my $self = bless {
        command   => $args{command},
        columns   => $args{columns} || [],
        group     => $args{group} || [],
        order     => $args{order} || [],
        limit     => $args{limit} || 0,
        offset    => $args{offset} || 0,
        type      => $args{type},
        name_hash => {},
        rows      => [],
        stmts     => {},
        times     => {},
        row_index => 0,
    }, $class;

    return $self;
}

sub add_stmt {
    my ($self, $stmt, $name) = @_;
    $self->{stmts}{$name} = $stmt;
    return $self;
}

sub execute {
    my ($self, @params) = @_;

    $self->{rows} = [];
    $self->{times} = {};
    $self->reset_row_index;

    foreach my $name (sort keys %{ $self->{stmts} }) {
        my $start = [gettimeofday];
        my $stmt = $self->{stmts}{$name};
        local $@;
        eval { $stmt->execute(@params); };
        if ($@) {
            next if ( $stmt->errstr =~ /Table '.*?' doesn't exist/);
            carp $stmt->errstr;
        }
        my $end   = [gettimeofday];
        $self->{times}{$name} = tv_interval $start, $end;

        next if ($self->{command} eq 'SET');

        if (@{ $self->{columns} } == 0 || $self->{columns}->[0]->{name} eq '*') {
            $self->{columns} = [];
            for my $name ( @{ $stmt->{NAME} }) {
                push @{ $self->{columns} }, {
                    column  => $name,
                    name    => $name,
                    command => 'NONE',
                };
            }
        }

        next unless $stmt->rows;

        while (my @row = $stmt->fetchrow_array) {
            push @row, $name;
            push @{$self->{rows}}, \@row;
        }
    }

    $self->{name_hash} = {};
    for (my $i=0;$i<@{$self->{columns}};++$i) {
        $self->{name_hash}->{ $self->{columns}->[$i]->{name} } = $i;
    }

    $self->_aggregate();

    $self->group_by(@{$self->{group}});
    $self->order_by($_->{column}, $_->{order}) for reverse @{$self->{order}};
    $self->limit($self->{limit});

    return $self;
}

sub group_by {
    my ($self, @params) = @_;

    return $self unless @params;

    my @keys;
    push @keys, $self->{name_hash}{$_} foreach @params;

    my %rows;
    for my $row ( @{ $self->{rows} } ) {
        my @keyval;
        push @keyval, $row->[$_] foreach @keys;
        my $key = join "-", @keyval;
        if ($rows{$key}) {
            $rows{$key} = $self->_merge_row($rows{$key}, $row);
        } else {
            $rows{$key} = $row;
        }
    }

    my @result_row;
    push @result_row, $rows{$_} for (sort keys %rows);

    $self->{rows} = \@result_row;

    $self->order_by($_) for reverse @params;

    return $self;
}

sub order_by {
    my ($self, $param, $order) = @_;

    $order ||= "ASC";

    return $self unless ($self->{rows}->[0]);

    my $index = $self->{name_hash}{$param};

    if ($self->{rows}->[0]->[$index] =~ /^-?\d+\.?\d*$/) {
        if ($order eq 'DESC') {
            $self->{rows} = [ sort {$b->[$index] <=> $a->[$index]} @{$self->{rows}} ];
        } else {
            $self->{rows} = [ sort {$a->[$index] <=> $b->[$index]} @{$self->{rows}} ];
        }
    } else {
        if ($order eq 'DESC') {
            $self->{rows} = [ sort {$b->[$index] cmp $a->[$index]} @{$self->{rows}} ];
        } else {
            $self->{rows} = [ sort {$a->[$index] cmp $b->[$index]} @{$self->{rows}} ];
        }
    }

    return $self;
}

sub limit {
    my ($self, $limit, $offset) = @_;

    return $self unless $limit;

    $offset ||= 0;

    $self->{rows} = [ splice @{$self->{rows}}, $offset, $limit ];

    return $self;
}

sub rows {
    my $self = shift;

    scalar $self->get_all; 
}

sub get_all {
    my $self = shift;
    @{ $self->{rows} };
}

sub get_row {
    my $self = shift;
    $self->{rows}->{$self->{row_index}};
}

sub fetchrow {
    my $self = shift;
    return unless $self->has_next_row;
    $self->{rows}->[$self->next_row_index];
}

sub fetchrow_array {
    my $self = shift;
    return unless $self->has_next_row;
    @{$self->fetchrow};
}

sub fetchrow_arrayref {
    my $self = shift;
    $self->fetchrow;
}

sub fetchrow_hash {
    my $self = shift;
    my %row;
    my $index = 0;

    return unless $self->has_next_row;

    for my $col ($self->fetchrow_array) {
        if ($index == scalar(@{$self->{columns}})) {
            $row{claster} = $col;
        } else {
            my $name = $self->{columns}->[$index++]->{name};
            $row{$name} = $col;
        }
    }

    return %row;
}

sub fetchrow_hashref {
    my $self = shift;
    return unless $self->has_next_row;
    my %row = $self->fetchrow_hash;
    \%row;
}

sub names {
    my $self = shift;

    my @names;
    for my $column (@{$self->{columns}}) {
        push @names, $column->{name};
    }
    push @names, 'claster';

    \@names;
}

sub next_row_index {
    my $self = shift;
    $self->{row_index}++;
}

sub reset_row_index {
    my $self = shift;
    $self->{row_index} = 0;
}

sub has_next_row {
    my $self = shift;
    return $self->{row_index} < $self->rows;
}

sub last_exec_times {
    my $self = shift;
    return $self->{times};
}

sub _aggregate {
    my ($self) = @_;

    for my $column (@{$self->{columns}}) {
        return $self unless (grep {$_ eq $column->{command}} AGGREGATE_FUNCTIONS);
    }

    return $self if @{$self->{group}};
    return $self if @{$self->{order}};
    return $self if $self->{limit};
    return $self if (@{ $self->{rows} } > keys %{$self->{stmts}});

    my $result;
    for my $row ( @{ $self->{rows} } ) {
        if ($result) {
            $result = $self->_merge_row($result, $row);
        } else {
            $result = $row;
        }
    }
    $self->{rows} = [$result];
    return $self;
}

sub _merge_row {
    my ($self, $row, $add) = @_;
    
    for (my $i=0;$i<@{$self->{columns}};++$i) {
        my $command = $self->{columns}->[$i]->{command};
        if ($command eq "COUNT" || $command eq "SUM") {
            $row->[$i] = ($row->[$i] || 0) + ($add->[$i] || 0);
        } elsif ($command eq "MAX") {
            $row->[$i] = $add->[$i] if ($row->[$i] < $add->[$i]);
        } elsif ($command eq "MIN") {
            $row->[$i] = $add->[$i] if ($row->[$i] > $add->[$i]);
        } elsif ($command eq "NONE") {
            # 何もしない
        } else {
            # 何も考えていない
        }
    }

    my $last_index = scalar @{$self->{columns}};

    if ($row->[$last_index] ne $add->[$last_index]) {
        $row->[$last_index] = join ",", $row->[$last_index], $add->[$last_index];
    }

    return $row;
}

1;
__END__

=head1 NAME

MySQL::Sharding::Client::ResultSet - perl module to used by MySQL::Sharding::Client.

=head1 VERSION

This document describes MySQL::Sharding::Client::Client version 0.0.1.

=head1 SYNOPSIS

    use MySQL::Sharding::Client::ResultSet;

    my $sth = MySQL::Sharding::Client::ResultSet->new(
        command => 'SELECT',
        %arguments,
    );

    $sth->add_stmt( $_->prepare($statement) ) for @dbhs;

    $sth->execute(@bind_value);

    $rv = $sth->rows;

    $sth->group_by(@column_names);

    $sth->order_by($column_name, 'ASC');
    $sth->order_by($column_name, 'DESC');

    $sth->limit($num);

    @row_ary  = $sth->fetchrow_array;
    %row_hash = $sth->fetchrow_hash;
    $ary_ref  = $sth->fetchrow_arrayref;
    $hash_ref = $sth->fetchrow_hashref;

    $sth->disconnect;

=head1 DESCRIPTION

This Module instance returned by MySQL::Sharding::Client->prepare
or MySQL::Sharding::Client->do.

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
