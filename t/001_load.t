use Test::More tests => 5;
use Test::Exception;
use Module::Build;
use strict;
use warnings;
use REST::Neo4p;
use DBI;
use lib '../lib';

my $build;
my ($user,$pass);
eval {
  $build = Module::Build->current;
  $user = $build->notes('user');
  $pass = $build->notes('pass');
};
diag $user;
my $TEST_SERVER = $build ? $build->notes('test_server') : 'http://127.0.0.1:7474';
my $num_live_tests = 5;

my $dbh;
my ($host,$port) = $TEST_SERVER =~ m|.*//([^:]+):([0-9]+)$|;
my $connected;
eval {
 $connected = REST::Neo4p->connect($TEST_SERVER, $user, $pass);
};
SKIP : {
  skip 'no connection to neo4j', $num_live_tests unless $connected;
  my $dsn = "dbi:Neo4p:db=$TEST_SERVER";
  ok $dbh = DBI->connect($dsn,$user,$pass), 'create Neo4p dbh with full url';
  ok $dbh->ping, 'ping';
  like $dbh->neo_neo4j_version, qr/^[0-9]+\.[0-9]+/, 'neo4j version retrieved';
  $dbh->disconnect;
  ok $dbh = DBI->connect("dbi:Neo4p:db=$host:$port",$user,$pass,
		      {RaiseError => 1}), 'create Neo4p dbh with db';
  $dbh->disconnect;
  ok $dbh = DBI->connect("dbi:Neo4p:host=$host;port=$port",$user,$pass,
		       {RaiseError => 1}), 'create Neo4p dbh with host, port';
  $dbh->disconnect;
}

