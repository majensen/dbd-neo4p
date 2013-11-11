use Test::More;
use lib 'lib';
use lib 't/lib';
use REST::Neo4p;
use Neo4p::Test;
#$SIG{__DIE__} = sub { $DB::single=1; $_[0] !~ /malformed j/i && print $_[0] };
my $build;
my ($user, $pass);
eval {
  $build = Module::Build->current;
  $user = $build->notes('user');
  $pass = $build->notes('pass');
};
my $TEST_SERVER = $build ? $build->notes('test_server') : 'http://127.0.0.1:7474';
my $num_live_tests = 11;

ok $t = Neo4p::Test->new($TEST_SERVER, $user, $pass), 'new test object';
isa_ok ($t, 'Neo4p::Test');

SKIP : {
  skip 'no connection to neo4j', $num_live_tests unless $t;
  ok $t->create_sample, 'create sample graph';
  is $t->nix->find_entries('name:*'), 5, 'all sample nodes created';
  is $t->rix->find_entries('hash:*'), 6, 'all sample relns created';
  ok $t->create_sample, 'create again, and should still have...';
  is $t->nix->find_entries('name:*'), 5, 'same num of sample nodes and...';
  is $t->rix->find_entries('hash:*'), 6, 'same num sample relns';
  ok $t->delete_sample, 'delete the sample graph';
  ok !REST::Neo4p->get_index_by_name("N".$t->uuid, 'node'), 'node idx is gone';
  ok !REST::Neo4p->get_index_by_name("R".$t->uuid, 'relationship'), 'reln idx is gone';
  my $q = REST::Neo4p::Query->new("MATCH n WHERE n.uuid! = '".$t->uuid."' RETURN n");
  $q->execute;
  ok !$q->err, 'query executed';
  ok !$q->fetch, 'nodes removed';
}
done_testing;
