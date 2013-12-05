use Test::More tests => 6;
use Test::Exception;
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
my $TEST_SERVER = $build ? $build->notes('test_server') : 'http://127.0.0.1:7474';
my $num_live_tests = 6;

my $dbh;
my ($host,$port) = $TEST_SERVER =~ m|.*//([^:]+):([0-9]+)$|;
my $connected = REST::Neo4p->connect($TEST_SERVER, $user, $pass);
SKIP : {
  skip 'no connection to neo4j', $num_live_tests unless $connected;
  ok $dbh = DBI->connect("dbi:Neo4p:db=$TEST_SERVER"), 
    'create Neo4p dbh with full url';
  ok $dbh->ping, 'ping';
  like $dbh->x_neo4j_version, qr/^[0-9]+\.[0-9]+/, 'neo4j version retrieved';
  $dbh->disconnect;
  ok $dbh = DBI->connect("dbi:Neo4p:db=$host:$port",
		      {RaiseError => 1}), 'create Neo4p dbh with db';
  $dbh->disconnect;
  ok $dbh = DBI->connect("dbi:Neo4p:host=$host;port=$port",
		       {RaiseError => 1}), 'create Neo4p dbh with host, port';
  $dbh->disconnect;
  TODO : {
    local $TODO = "Can't get response from the DBI guys";
    ok $dbh->can('neo_neo4j_version'), "get neo4j server version";
  }
}

