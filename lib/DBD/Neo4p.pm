use v5.10.0;
package DBD::Neo4p;
use strict;
use warnings;
use REST::Neo4p 0.2120;
require DBI;

our $VERSION = '0.0001';
our $err = 0;               # holds error code   for DBI::err
our $errstr =  '';          # holds error string for DBI::errstr
our $drh = undef;           # holds driver handle once initialised
our $prefix = 'neo';

sub driver($$){
#0. already created - return it
    return $drh if $drh;
#1. not created(maybe normal case)
    my($sClass, $rhAttr) = @_;
    $sClass .= '::dr';

# install methods if nec.
#    DBD::Neo4p::db->install_method('drv_example_dbh_method');
    DBD::Neo4p::db->install_method('x_neo4j_version');

    $drh = DBI::_new_drh($sClass,  
        {   Name        => $sClass,
            Version     => $VERSION,
            Err         => \$DBD::Neo4p::err,
            Errstr      => \$DBD::Neo4p::errstr,
#            State       => \$DBD::Neo4p::sqlstate,
            Attribution => 'DBD::Neo4p by Mark A. Jensen'
        }
    );
    return $drh;
}

package DBD::Neo4p::dr;
$DBD::Neo4p::dr::imp_data_size = 0;

sub connect($$;$$$) {
    my($drh, $sDbName, $sUsr, $sAuth, $rhAttr)= @_;

#1. create database-handle
    my ($outer, $dbh) = DBI::_new_dbh($drh, {
        Name         => $sDbName,
        USER         => $sUsr,
        CURRENT_USER => $sUsr,
    });
    # default attributes
    $dbh->STORE('neo_ResponseAsObjects',0);

#2. Parse extra strings in DSN(key1=val1;key2=val2;...)
    foreach my $sItem (split(/;/, $sDbName)) {
      my ($key, $value) = $sItem =~ /(.*?)=(.*)/;
      return $drh->set_err($DBI::stderr, "Can't parse DSN part '$sItem'")
            unless defined $value;
      $key = "${prefix}_$key" unless $key =~ /^${prefix}_/;
      $dbh->STORE($key, $value);
    }

    my $db = delete $rhAttr->{"${prefix}_database"} || delete $rhAttr->{"${prefix}_db"};
    my $host = delete $rhAttr->{"${prefix}_host"} || 'localhost';
    my $port = delete $rhAttr->{"${prefix}_port"} || 7474;
    my $protocol = delete $rhAttr->{"${prefix}_protocol"} || 'http';
    my $user = delete $rhAttr->{"${prefix}_user"};
    my $pass = delete $rhAttr->{"${prefix}_pass"} || delete $rhAttr->{"${prefix}_password"};
    # use db=<protocol>://<host>:<port> or host=<host>;port=<port>
    # db attribute trumps
    if ($db) {
      ($protocol, $host, $port) = $db =~ m|^(https?)?(?:://)?([^:]+):?([0-9]*)$|;
      $protocol //= 'http';
      return $drh->set_err($DBI::stderr, "DB host and/or port not specified correctly") unless ($host && $port);
    }

    # real connect...

    $db = "$protocol://$host:$port";
    eval {
      REST::Neo4p->connect($db,$user,$pass) or die "Can't connect";
    };
    if (my $e = Exception::Class->caught()) {
      return
	ref $e ? $drh->set_err($DBI::stderr, "Can't connect to $sDbName: ".ref($e)." : ".$e->message.' ('.$e->code.')') :
	  $drh->set_err($DBI::stderr, $e);
    };

    foreach my $sKey (keys %$rhAttr) {
        $dbh->STORE($sKey, $rhAttr->{$sKey});
    }
    $dbh->STORE(Active => 1);
    $dbh->STORE(AutoCommit => 1);
    $dbh->{"${prefix}_agent"} = $REST::Neo4p::AGENT;

    return $outer;
}


# FIXME: data_source not yet supported
sub data_sources ($;$) {
    my($drh, $rhAttr) = @_;
    return;
}

sub disconnect_all($) { }

package DBD::Neo4p::db;
$DBD::Neo4p::db::imp_data_size = 0;

sub prepare {
    my($dbh, $sStmt, $rhAttr) = @_;
#1. Create blank sth
    my ($outer, $sth) = DBI::_new_sth($dbh, { Statement   => $sStmt, });
    return $sth unless($sth);

# cypher query parameters are given as tokens surrounded by curly braces:
# crude count:
    my @parms = $sStmt =~ /\{\s*([^}[:space:]]*)\s*\}/g;
    $sth->STORE('NUM_OF_PARAMS', scalar @parms);
    $sth->{"${prefix}_param_names"} = \@parms;
    $sth->{"${prefix}_param_values"} = [];
    return $outer;
}

sub commit ($) {
    my($dbh) = @_;
    if ($dbh->FETCH('AutoCommit')) {
      warn("Commit ineffective while AutoCommit is on") if $dbh->FETCH('Warn');
      return;
    }
    else {
      warn("Transactions not yet supported by REST::Neo4p (commit)") if $dbh->FETCH('Warn');
      return;
    }
}

sub rollback ($) {
    my($dbh) = @_;
    if ($dbh->FETCH('AutoCommit')) {
      warn("Rollback ineffective while AutoCommit is on") if $dbh->FETCH('Warn');
      return;
    }
    else {
      warn("Transactions not yet supported by REST::Neo4p (rollback)") if $dbh->FETCH('Warn');
      return;
    }
}

sub ping {
  my $dbh = shift;
  my $sth = $dbh->prepare('MATCH a RETURN str(1) LIMIT 1') or return 0;
  $sth->execute or return 0;
  $sth->finish;
  return 1;
}

# neo4j metadata -- needs thinking
# v2.0 : http://docs.neo4j.org/chunked/2.0.0-M06/rest-api-cypher.html#rest-api-retrieve-query-metadata

sub x_neo4j_version {
  my $dbh = shift;
  return $dbh->{"${prefix}_agent"}->{_actions}{neo4j_version};
}

#>>>>> table_info (DBD::Template::db) -----------------------------------------------
sub table_info ($) {
    my($dbh) = @_;
#-->> Change
    my ($raTables, $raName) =
            &{$dbh->{tmpl_func_}->{table_info}}($dbh)
                        if(defined($dbh->{tmpl_func_}->{table_info}));
#<<-- Change
    return undef unless $raTables;
#2. create DBD::Sponge driver
    my $dbh2 = $dbh->{'_sponge_driver'};
    if (!$dbh2) {
        $dbh2 = $dbh->{'_sponge_driver'} = DBI->connect("DBI:Sponge:");
        if (!$dbh2) {
            $dbh->DBI::set_err( 1, $DBI::errstr);
            return undef;
            $DBI::errstr .= ''; #Just for IGNORE warning
        }
    }
#3. assign table info to the DBD::Sponge driver
    my $sth = $dbh2->prepare("TABLE_INFO",
            { 'rows' => $raTables, 'NAMES' => $raName });
    if (!$sth) {
        $dbh->DBI::set_err(1, $dbh2->errstr());
    }
    return  $sth;
}

sub type_info_all ($) {
    my ($dbh) = @_;
    return [];
}

sub disconnect ($) {
    my ($dbh) = @_;
    $dbh->STORE(Active => 0);
    1;
}

sub FETCH ($$) {
  my ($dbh, $sAttr) = @_;
  given ($sAttr) {
    when ('AutoCommit') { return $dbh->{$sAttr} }
    when (/^${prefix}_/) { return $dbh->{$sAttr} }
    default { return $dbh->SUPER::FETCH($sAttr) }
  }
}

sub STORE ($$$) {
  my ($dbh, $sAttr, $sValue) = @_;
  given ($sAttr) {
    when ('AutoCommit') {
      if(defined($dbh->{tmpl_func_}->{rollback})) {
	$dbh->{$sAttr} = ($sValue)? 1: 0;
      }
      else{
	#Rollback
	warn("Can't disable AutoCommit with no rollback func", -1)
	  unless($sValue);
	$dbh->{$sAttr} = 1;
      }
      return 1;
    }
    # private attributes (neo_)
    when (/^${prefix}_/) {
      $dbh->{$sAttr} = $sValue;
      return 1;
    }
    default {
      return $dbh->SUPER::STORE($sAttr => $sValue);
    }
  }
}

sub DESTROY($) {
  my($dbh) = @_;
  # deal with the REST::Neo4p object
}

package DBD::Neo4p::st;
$DBD::Neo4p::st::imp_data_size = 0;

sub bind_param ($$$;$) {
  my($sth, $param, $value, $attribs) = @_;
  return $sth->DBI::set_err(2, "Can't bind_param $param, too big")
    if ($param > $sth->FETCH('NUM_OF_PARAMS'));
  $sth->{"${prefix}_param_values"}->[$param-1] = $value;
  return 1;
}

sub execute($@) {
  my ($sth, @bind_values) = @_;

  $sth->finish if $sth->{Active}; # DBI::DBD example, follow up...

  my $params = @bind_values ? \@bind_values : $sth->{"${prefix}_param_values"};
  unless (@$params == $sth->FETCH('NUM_OF_PARAMS')) {
    return $sth->set_err($DBI::stderr, "Wrong number of parameters");
  }
  # Execute
  # by this time, I know all my parameters
  # so create the Query obj here
  my %params;
  @params{@{$sth->{"${prefix}_param_names"}}} = @$params;
  my $q = $sth->{"${prefix}_query_obj"} = REST::Neo4p::Query->new(
    $sth->{Statement}, \%params
   );
  $q->{ResponseAsObjects} = $sth->{Database}->{neo_ResponseAsObjects};

  my $numrows = $q->execute;
  if ($q->err) {
    return $sth->set_err($DBI::stderr,$q->errstr.' ('.$q->err.')');
  }

#4. AutoCommit - handle this later
#    $sth->{Database}->commit if($sth->{Database}->FETCH('AutoCommit'));
  $sth->{"${prefix}_rows"} = $numrows;
  # don't know why I have to do the following, when the FETCH 
  # method delegates this to the query object and $sth->{NUM_OF_FIELDS}
  # thereby returns the correct number, but $sth->_set_bav($row) segfaults
  # if I don't:
  $sth->STORE(NUM_OF_FIELDS => 0);
  $sth->STORE(NUM_OF_FIELDS => $q->{NUM_OF_FIELDS});

  $sth->{Active} = 1;
  return $numrows || '0E0';
}

sub fetch ($) {
  my ($sth) = @_;
  my $q =$sth->{"${prefix}_query_obj"};
  unless ($q) {
    return $sth->set_err($DBI::stderr, "Query not yet executed");
  }
  my $row;
  eval {
    $row = $q->fetch;
  };
  if (my $e = Exception::Class->caught) {
    $sth->finish;
    return $sth->set_err($DBI::stderr, ref $e ? ref($e)." : ".$e->message : $e);
  }
  if ($q->err) {
    $sth->finish;
    return $sth->set_err($DBI::stderr,$q->errstr.' ('.$q->err.')');
  }
  
  unless ($row) {
    $sth->STORE(Active => 0);
    return undef;
  }
  $sth->_set_fbav($row);
}
*fetchrow_arrayref = \&fetch;

sub rows ($) {
  my($sth) = @_;
  return $sth->{"${prefix}_rows"};
}

sub finish ($) {
  my ($sth) = @_;
  $sth->{"${prefix}_query_obj"} = undef;
  $sth->STORE(Active => 0);
  $sth->SUPER::finish();
  return 1;
}

sub FETCH ($$) {
  my ($sth, $attrib) = @_;
  my $q =$sth->{"${prefix}_query_obj"};
  given ($attrib) {
    when ('NAME') { return ($q && $q->{NAME}) }
    when ('NUM_OF_FIELDS') { return ($q && $q->{NUM_OF_FIELDS}) }
    when ('TYPE') {
      return [(DBI::SQL_VARCHAR()) x $sth->FETCH('NUM_OF_FIELDS')]
    }
    when ('PRECISION') {
      return [(-1) x $sth->FETCH('NUM_OF_FIELDS')]
    }
    when ('SCALE') {
      return [(undef) x $sth->FETCH('NUM_OF_FIELDS')]
    }
    when ('NULLABLE') {
      return [(1) x $sth->FETCH('NUM_OF_FIELDS')]
    }
    when ('RowInCache') {
      return
    }
    when ('CursorName') {
      return
    }
    # Private driver attributes have neo_ prefix
    when (/^${prefix}_/) {
      return $sth->{$attrib}
    }
    default {
      return $sth->SUPER::FETCH($attrib)
    }
  }
}

sub STORE ($$$) {
  my ($sth, $attrib, $value) = @_;
  #1. Private driver attributes have neo_ prefix
  given ($attrib) {
    when (/^${prefix}_/) { 
      $sth->{$attrib} = $value;
      return 1;
    }
    default {
      return $sth->SUPER::STORE($attrib, $value);
    }
  }
}

sub DESTROY {
  my ($sth) = @_;
  undef $sth->{"${prefix}_query_obj"};
}

#>> Just for no warning-----------------------------------------------
$DBD::Neo4p::dr::imp_data_size = 0;
$DBD::Neo4p::db::imp_data_size = 0;
$DBD::Neo4p::st::imp_data_size = 0;
*DBD::Neo4p::st::fetchrow_arrayref = \&DBD::Neo4p::st::fetch;
#<< Just for no warning------------------------------------------------
1;
__END__

=head1 NAME

DBD::Neo4p - A DBI driver for Neo4j via REST::Neo4p

=head1 SYNOPSIS

 use DBI;
 my $dbh = DBI->connect("dbi:Neo4p:http://127.0.0.1:7474;user=foo;pass=bar");
 my $q =<<CYPHER;
 START x = node:node_auto_index(name= { startName })
 MATCH path =(x-[r]-friend)
 WHERE friend.name = { name }
 RETURN TYPE(r)
 CYPHER
 my $sth = $dbh->prepare($q);
 $sth->execute("I", "you"); # startName => 'I', name => 'you'
 while (my $row = $sth->fetch) {
   print "I am a ".$row->[0]." friend of yours.\n";
 }

=head1 DESCRIPTION

L<DBD::Neo4p> is a L<DBI>-compliant wrapper for L<REST::Neo4p::Query>
that allows for the execution of Neo4j Cypher language queries against
a L<Neo4j|www.neo4j.org> graph database.

=head1 Functions

=head2 Driver Level

=over

=item connect

 my $dbh = DBI->connect("dbi:Neo4p:db=http://127.0.0.1:7474");
 $dbh = DBI->connect("dbi:Neo4p:host=127.0.0.1;port=7474");
 $dbh = DBI->connect("dbi:Neo4p:db=http://127.0.0.1:7474;user=me;pass=s3kr1t");

=back

=head2 Database Level

=over 4

=item prepare

=item commit

=item rollback

=item disconnect

 $dbh->disconnect

=item table_info

=item type_info

=item quote

=item x_neo4j_version

 say "Neo4j Server Version ".$dbh->x_neo4j_version;

Get the neo4j server version.

=back

=head2 Statement Level

=over

=item execute

=item fetch

=item rows

=item finish

=back

=head1 ATTRIBUTES

=head2 Database Handle Attributes

=over

=item ResponseAsObjects

 $dbh->{ResponseAsObjects}

If set, columns that are nodes, relationships or paths are returned 
as L<REST::Neo4p> objects of the appropriate type.

If clear (default), these entities are returned as hash or array refs,
as appropriate.  For descriptions of these, see
L<REST::Neo4p::Node/as_simple()>,
L<REST::Neo4p::Relationship/as_simple()>, and
L<REST::Neo4p::Path/as_simple()>.

=back

=head2 Statement Handle Attributes

=over

=item neo_param_names

 @param_names = @{ $sth->{neo_param_names} };

Arrayref of named parameters in statement.

=item neo_param_values
 
 @param_values = @{ $sth->{neo_param_values} };

Arrayref of bound parameter values.

=back

=head1 SEE ALSO

L<REST::Neo4p>, L<REST::Neo4p::Query>, L<DBI>, L<DBI::DBD>

=head1 AUTHOR

 Mark A. Jensen
 CPAN ID : MAJENSEN
 majensen -at- cpan -dot- org

=head1 COPYRIGHT

 (c) 2013 by Mark A. Jensen

=head1 LICENSE

Copyright (c) 2013 Mark A. Jensen. This program is free software; you
can redistribute it and/or modify it under the same terms as Perl
itself.

=cut
