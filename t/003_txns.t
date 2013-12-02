use Test::More;
use Test::Exception;
use lib 't/lib';
use lib '../lib';
use lib '../t/lib';
use strict;
use warnings;
use REST::Neo4p;
use Neo4p::Test;
use DBI;

my $build;
my ($user,$pass);
eval {
  $build = Module::Build->current;
  $user = $build->notes('user');
  $pass = $build->notes('pass');
};
my $TEST_SERVER = $build ? $build->notes('test_server') : 'http://127.0.0.1:7474';
my $num_live_tests = 1;
my $t;
my $dsn = "dbi:Neo4p:db=$TEST_SERVER";
$dsn .= ";user=$user;pass=$pass" if defined $user;
ok my $dbh = DBI->connect($dsn);

SKIP : {
  skip 'no connection to neo4j', $num_live_tests unless $dbh->ping;
  REST::Neo4p->set_handle($dbh->{neo_Handle});
  skip 'Need server v2.0 to test transactions', $num_live_tests unless REST::Neo4p->_check_version(2,0,0,2);
  $t = Neo4p::Test->new();
  ok $t->create_sample, 'create sample graph';
  my $idx = ${$t->nix};
  my $q =<<CYPHER;
   START x = node:$idx(name='I')
   MATCH path =(x-[r]-friend)
   WHERE friend.name = 'you'
   RETURN r
CYPHER
  ok $dbh->{AutoCommit}, "AutoCommit defaults to set";
  $dbh->{AutoCommit} = 0;
  ok !$dbh->begin_work, "try to begin_work";
  like $DBI::errstr, qr/begin_work not effective/, "AutoCommit cleared, begin_work not effective";
  ok my $sth = $dbh->prepare($q), 'prepare query';
  ok $sth->execute, 'execute';
#  ok !$sth->fetch, "nothing fetched before commit";
  ok $dbh->commit;
  
}

done_testing;

END {
  $dbh->disconnect;
}
