use Test::More;
use Test::Exception;
use strict;
use warnings;
use REST::Neo4p;
use DBI;
use lib '../lib';

my $build;
eval {
  $build = Module::Build->current;
};
my $TEST_SERVER = $build ? $build->notes('test_server') : 'http://127.0.0.1:7474';
my $num_live_tests = 1;

ok my $dbh = DBI->connect("dbi:Neo4p:db=$TEST_SERVER");

SKIP : {
  skip 'no connection to neo4j', $num_live_tests unless $dbh->ping;
  $dbh->disconnect;
}

done_testing;

