requires 'DBI';
requires 'Term::ReadLine';
requires 'YAML::Syck' => 1.00;
requires 'Path::Class';
requires 'Time::HiRes';

on 'test' => sub {
    requires 'Test::More'     => '0.88';
    requires 'Test::Requires' => '0.06';
    requires 'Test::mysqld';
};
