use Test::More;
use Test::Exception;
use strict;
use warnings;
use DBI;
use lib '../lib';

my $build;
eval {
  $build = Module::Build->current;
};
my $TEST_SERVER = $build ? $build->notes('test_server') : 'http://127.0.0.1:7474';
my $num_live_tests = 3;

my $dbh;
my ($host,$port) = $TEST_SERVER =~ m|.*//([^:]+):([0-9]+)$|;
ok $dbh = DBI->connect("dbi:Neo4p:db=$TEST_SERVER",
		      {RaiseError => 1}), 'create Neo4p dbh with full url';
ok my $connected = $dbh->ping, 'ping';
SKIP : {
  skip 'no connection to neo4j', $num_live_tests unless $connected;
  like $dbh->x_neo4j_version, qr/^[0-9]+\.[0-9]+/, 'neo4j version retrieved';
  $dbh->disconnect;
  ok $dbh = DBI->connect("dbi:Neo4p:db=$host:$port",
		      {RaiseError => 1}), 'create Neo4p dbh with db';
  $dbh->disconnect;
  ok $dbh = DBI->connect("dbi:Neo4p:host=$host;port=$port",
		       {RaiseError => 1}), 'create Neo4p dbh with host, port';

  $dbh->disconnect;
}

done_testing();
