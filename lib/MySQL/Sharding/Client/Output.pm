package MySQL::Sharding::Client::Output;

use strict;
use warnings;

use MySQL::Sharding::Client;
use YAML::Syck;

sub new {
    my ($class, %args) = @_;

    my $handler_options;
    if ($args{yaml}) {
        local $YAML::Syck::ImplicitTyping = 1;
        local $YAML::Syck::SingleQuote    = 1;
        $handler_options = YAML::Syck::LoadFile($args{yaml});
    } else {
        $handler_options = \%args;
    }

    my $self = bless {
        handler_options => $handler_options,
    }, $class;

    return $self;
}


sub execute {
    my ($self, $sql) = @_;
    my $handler = MySQL::Sharding::Client->connect(
        %{ $self->{handler_options} }
    );

    my $separator = "\t";

    my $rs = $handler->do($sql);

    print join $separator, @{ $rs->names };
    print "\n";
    while (my @row = $rs->fetchrow_array) {
        next unless @row;
        print join $separator, map {defined $_ ? $_ : "" } @row;
        print "\n";
    }
}

1;
__END__

=head1 NAME

MySQL::Sharding::Client::Output - perl module to do use MySQL::Sharding::Client by STDIN.

=head1 VERSION

This document describes MySQL::Sharding::Client::Output version 0.0.1.

=head1 SYNOPSIS

    use MySQL::Sharding::Client::Output;

    # create by MySQL::Sharding::Client connect options
    $prompt = MySQL::Sharding::Client::Output->new(
        %handler_options,
    );

    # create by yaml file.
    $prompt = MySQL::Sharding::Client::Output->new(
        yaml => 'path/to/connect_infos.yaml',
    );

    $prompt->execute($sql);

=head1 DESCRIPTION

this module used by sharding_prompt

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
