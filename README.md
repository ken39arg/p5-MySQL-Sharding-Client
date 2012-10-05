MySQL::Sharding::Client
=============

Perl extention to do can be use as one DBI handle to many DBI handles.
And prompt for handle any databases.


INSTALLATION
---------

install cpanm and then run the following command to install
MySQL::Sharding::Client:

    $ git clone git@github.com:ken39arg/p5-MySQL-Sharding-Client.git 
    $ cd p5-MySQL-Sharding-Client
    $ cpanm .

If you get an archive of this distribution, unpack it and build it
as per the usual:

    $ tar xzf  MySQL-Sharding-Client-$version.tar.gz
    $ cd MySQL-Sharding-Client-$version
    $ perl Makefile.PL
    $ make && make test


SETTING PROMPT
----------

add ${HOME}/.sharding_prompt.yml

### example

    connect_infos:
      name1:
        dsn: DBI:mysql:database=dbname1;host=localhost; 
      name2:
        dsn: DBI:mysql:database=dbname2;host=localhost; 
        user: username
        password: password 
        options:
          RaiseError: 1
      name3:
        dsn: DBI:mysql:database=dbname3;host=localhost; 
    
    user:     root
    password: ''
    pre_commands:
      - set names utf8
      - set sql_big_selects=1 

HOWTO USE PROMPT
----------

    $ shard_prompt --config=<path/to/config.yaml>
    $ shard_prompt --help
    $ shard_prompt

and input SQL.


SUPPORTING COMMAND 
----------

### DML

`SELECT` only.


### COMMAND

  * `COUNT`
  * `SUM`
  * `MAX`
  * `MIN`


DOCUMENTATION
----------

    $ perldoc MySQL::Sharding::Client


AUTHOR
----------

Kensaku Araga E<lt>ken39arg {at} gmail.com<gt>


LICENSE AND COPYRIGHT
----------

Copyright (c) 2012, Kensaku Araga. All rights reserved.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

