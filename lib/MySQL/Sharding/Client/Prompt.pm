package MySQL::Sharding::Client::Prompt;

use strict;
use warnings;

use Term::ReadLine;
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

sub run {
    my ($self, %args) = @_;
    my $handler = MySQL::Sharding::Client->connect(
        %{ $self->{handler_options} }
    );

    for my $name (keys %{ $self->{handler_options}{connect_infos} }) {
        print "$name --> connect --> ";
        print($handler->dbh($name)->ping ? "OK" : "NG");
        print "\n";
    }

    my $term  = Term::ReadLine->new(
        $args{title} || "MySQL::Sharding::Client::Prompt",
    );
    my $prompt = $args{prompt} || "sharding> ";
    my $OUT = $term->OUT || \*STDOUT;

    my $sql = "";
    while ( defined ($_ = $term->readline($prompt)) ) {
        $sql .= $_;

        while (1) {
            if ($sql =~ m/(.*?);(.*)/) {
                my $exec_sql = $1;
                $sql = $2;
                local $@;
                eval {
                    my $rs = $handler->do($exec_sql);
                    __print_row($OUT, $rs);
                };
                if ($@) {
                    print $OUT "$@\n";
                }
            } else {
                last;
            }
        }
        $term->addhistory($_) if /\S/;
    }
}

sub __print_row {
    my ($OUT, $rs) = @_;

    my @size;
    my $index = 0;
    my $names = $rs->names;
    my $i = 0;
    foreach my $name ( @$names ) {
        $size[$index] = length $name;
        $index++;
    }
    
    while (my @row = $rs->fetchrow_array) {
        for (my $i = 0; $i < $index; $i++) {
            $size[$i] = length($row[$i]) if ($size[$i] < length($row[$i]));
        }
    }

    $rs->reset_row_index;

    my $sepalater = "+";
    for ($i = 0; $i < $index; $i++) {
        for (my $j=0;$j<$size[$i];++$j) {
            $sepalater .= "-";
        }
        $sepalater .= "--+";
    }
    print $OUT "$sepalater\n";

    my $line = "|";
    for ($i = 0; $i < $index; $i++) {
        $line .= sprintf " %-".$size[$i]."s |", $names->[$i];
    }
    print $OUT "$line\n";
    print $OUT "$sepalater\n";
    while (my @row = $rs->fetchrow_array) {
        $line = "|";
        for ($i = 0; $i < $index; $i++) {
            $line .= sprintf(" %".$size[$i]."s |", defined $row[$i] ? $row[$i] : "" );
        }
        print $OUT "$line\n";
    }

    print $OUT "$sepalater\n";

    my $last_exec_times = $rs->last_exec_times;
    for my $shard (keys %$last_exec_times) {
        print $OUT "$shard: (" . $last_exec_times->{$shard} . " sec)\n";
    }

    print $OUT "\n";
}

1;
