require 5.008;
package DBD::Neo4p;
use strict;
use warnings;
use REST::Neo4p;
use Try::Tiny;
require DBI;

our $VERSION = '0.001';      
our $err = 0;               # holds error code   for DBI::err
our $errstr =  '';          # holds error string for DBI::errstr
our $drh = undef;           # holds driver handle once initialised

#>>>>> driver (DBD::Template) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
sub driver($$){
#0. already created - return it
    return $drh if $drh;
#1. not created(maybe normal case)
    my($sClass, $rhAttr) = @_;
    $sClass .= '::dr';

# install methods if nec.
#    DBD::Neo4p::db->install_method('drv_example_dbh_method');

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

#>>>>> connect (DBD::Template::dr) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
sub connect($$;$$$) {
    my($drh, $sDbName, $sUsr, $sAuth, $rhAttr)= @_;
#1. create database-handle
    my ($outer, $dbh = DBI::_new_dbh($drh, {
        Name         => $sDbName,
        USER         => $sUsr,
        CURRENT_USER => $sUsr,
    });

    my $prefix = 'neo_';

#2. Parse extra strings in DSN(key1=val1;key2=val2;...)
    foreach my $sItem (split(/;/, $sDbName)) {
      my ($key, $value) = $sItem =~ /(.*?)=(.*)/;
      return $drh->set_err($DBI::stderr, "Can't parse DSN part '$sItem'")
            unless defined $value;
      $key = $prefix.$key unless $key =~ /^$prefix/;
#        $dbh->STORE($1, $2) if ($sItem =~ /(.*?)=(.*)/);
      $dbh->STORE($key, $value);
    }

    my $db = delete $rhAttr->{neo_database} || delete $attr->{neo_db};
    my $host = delete $rhAttr->{neo_host} || 'localhost';
    my $port = delete $rhAttr->{neo_port} || 7474;
    my $protocol = delete $rhAttr->{neo_protocol} || 'http';
    # use db=<protocol>://<host>:<port> or host=<host>;port=<port>
    # db attribute trumps
    if ($db) {
      ($protocol, $host, $port) = $db =~ m|^(https?)?(?:://)?([^:]+):?([0-9]*)$|;
      $protocol //= 'http';
      return $drh->set_err($DBI::stderr, "DB host and/or port not specified correctly") unless ($host && $port);
    }

#4. Initialize
    my @aReqF = qw(prepare execute fetch rows name);
    my @aMissing=();
    for my $sFunc (@aReqF) {
        push @aMissing, $sFunc unless(defined($dbh->{tmpl_func_}->{$sFunc}));
    }
    die "Set " . join(',', @aMissing) if(@aMissing);
 
    # real connect...

    $db = "$protocol://$host:$port";
    try {
      REST::Neo4p->connect($db);
    }
    catch {
      ref ? $drh->set_err($DBI::stderr, "Can't connect to $sDbName: ".ref." - ".$_->message) : 
	$drh->set_err($DBI::stderr, $_);
    };

    foreach my $sKey (keys %$rhAttr) {
        $dbh->STORE($sKey, $rhAttr->{$sKey});
    }
    $dbh->STORE(Active => 1);
    $dbh->STORE(AutoCommit => 1);
    $dbh->{neo_agent} = $REST::Neo4p::AGENT;

    return $outer;
}

#>>>>> data_sources (DBD::Template::dr) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# FIXME: data_source not yet supported
sub data_sources ($;$) {
    my($drh, $rhAttr) = @_;
    my $sDbdName = 'Neo4p';
    my @aDsns = ();
 
    @aDsns = &{$rhAttr->{tmpl_datasources}} ($drh)
        if(defined($rhAttr->{tmpl_datasources}));   #<<-- Change
 
    return (map {"dbi:$sDbdName:$_"} @aDsns);
}

#>>>>> disconnect_all (DBD::Template::dr) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
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
    my @parms = $sStmt =~ /\{([^}]*)\}/g;
    $sth->STORE('NUM_OF_PARAMS', scalar @parms);
    $sth->{neo_params} = \@parms;
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
#>>>>> tmpl_func_ (DBD::Template::db) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#-->>Change
sub tmpl_func($@) {
    my($dbh, @aRest) = @_;
    return unless($dbh->{tmpl_func_}->{funcs});
 
    my $sFunc = pop(@aRest);
    &{$dbh->{tmpl_func_}->{funcs}->{$sFunc}}($dbh, @aRest)
            if(defined($dbh->{tmpl_func_}->{funcs}->{$sFunc}));
}
#<<--Change

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
#>>>>> quote (DBD::Template::db) ----------------------------------------------------
sub quote ($$;$) {
    my($dbh, $sObj, $iType) = @_;
    return &{$dbh->{tmpl_func_}->{quote}}($dbh, $sObj, $iType)
                        if(defined($dbh->{tmpl_func_}->{quote}));   #Change
 
#1.Numeric
    if (defined($iType)  &&
        ($iType == DBI::SQL_NUMERIC()   || $iType == DBI::SQL_DECIMAL()   ||
         $iType == DBI::SQL_INTEGER()   || $iType == DBI::SQL_SMALLINT()  ||
         $iType == DBI::SQL_FLOAT()     || $iType == DBI::SQL_REAL()      ||
         $iType == DBI::SQL_DOUBLE()    || $iType == DBI::TINYINT())) {
        return $sObj;
    }
#2.NULL
    return 'NULL' unless(defined $sObj);
#3. Others
    $sObj =~ s/\\/\\\\/sg;
    $sObj =~ s/\0/\\0/sg;
    $sObj =~ s/\'/\\\'/sg;
    $sObj =~ s/\n/\\n/sg;
    $sObj =~ s/\r/\\r/sg;
    return "'$sObj'";
}

sub type_info_all ($) {
    my ($dbh) = @_;
    return [];
}

#>>>>> disconnect (DBD::Template::db) -----------------------------------------------
sub disconnect ($) {
    my ($dbh) = @_;
    &{$dbh->{tmpl_func_}->{disconnect}}($dbh)
                        if(defined($dbh->{tmpl_func_}->{disconnect}));
    1;
}


sub FETCH ($$) {
    my ($dbh, $sAttr) = @_;
# 1. AutoCommit
    return $dbh->{$sAttr} if ($sAttr eq 'AutoCommit');
# 2. lower cased = Driver private attributes
    return $dbh->{$sAttr} if ($sAttr eq (lc $sAttr));
# 3. pass up to DBI to handle
    return $dbh->SUPER::FETCH($sAttr);
}

sub STORE ($$$) {
    my ($dbh, $sAttr, $sValue) = @_;
#1. AutoCommit
    if ($sAttr eq 'AutoCommit') {
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
#2. Driver private attributes are lower cased
    elsif ($sAttr eq (lc $sAttr)) {
        $dbh->{$sAttr} = $sValue;
        return 1;
    }
#3. pass up to DBI to handle
    return $dbh->SUPER::STORE($sAttr, $sValue);
}

sub DESTROY($) {
    my($dbh) = @_;
    # something
}
 

package DBD::Neo4p::st;
$DBD::Neo4p::st::imp_data_size = 0;
#>>>>> bind_param (DBD::Template::st) -----------------------------------------------
sub bind_param ($$$;$) {
    my($sth, $param, $value, $attribs) = @_;
    return $sth->DBI::set_err(2, "Can't bind_param $param, too big")
        if ($param >= $sth->FETCH('NUM_OF_PARAMS'));
    $sth->{tmpl_params__}->[$param] = $value;  #<<Change (tmpl_)
    return 1;
}

#>>>>> execute (DBD::Template::st) --------------------------------------------------
sub execute($@) {
    my ($sth, @aRest) = @_;
#1. Set Parameters
#1.1 Get Parameters
    my ($raParams, @aRec);
    $raParams = (@aRest)? [@aRest] : $sth->{tmpl_params__};  #<<Change (tmpl_)
#1.2 Check Param count
    my $iParams = $sth->FETCH('NUM_OF_PARAMS');
    if ($iParams && scalar(@$raParams) != $iParams) { #CHECK FOR RIGHT # PARAMS.
        return $sth->DBI::set_err((scalar(@$raParams)-$iParams),
                "..execute: Wrong number of bind variables (".
                (scalar(@$raParams)-$iParams)." too many!)");
    }
#2. Execute
    my($oResult, $iNumFld, $sErr) =
        &{$sth->{Database}->{tmpl_func_}->{execute}}($sth, $raParams);
    if ($sErr) { return $sth->DBI::set_err( 1, $@); }
#3. Set NUM_OF_FIELDS
    if ($iNumFld  &&  !$sth->FETCH('NUM_OF_FIELDS')) {
        $sth->STORE('NUM_OF_FIELDS', $iNumFld);
    }
#4. AutoCommit
    $sth->{Database}->commit if($sth->{Database}->FETCH('AutoCommit'));
    return $oResult;
}
#>>>>> fetch (DBD::Template::st) ----------------------------------------------------
sub fetch ($) {
    my ($sth) = @_;
 
#1. get data
    my ($raDav, $bFinish, $bNotSel) =
        &{$sth->{Database}->{tmpl_func_}->{fetch}}($sth); #<<Change (tmpl_);
 
    return $sth->DBI::set_err( 1,
        "Attempt to fetch row from a Non-SELECT Statement") if ($bNotSel);
 
    if ($bFinish) {
        $sth->finish;
        return undef;
    }
 
    if ($sth->FETCH('ChopBlanks')) {
        map { $_ =~ s/\s+$//; } @$raDav;
    }
    $sth->_set_fbav($raDav);
}
*fetchrow_arrayref = \&fetch;
#>>>>> rows (DBD::Template::st) -----------------------------------------------------
sub rows ($) {
    my($sth) = @_;
    return &{$sth->{Database}->{tmpl_func_}->{rows}}($sth); #<<Change (tmpl_)
}
#>>>>> finish (DBD::Template::st) ---------------------------------------------------
sub finish ($) {
    my ($sth) = @_;
#-->> Change (if you want)
    &{$sth->{Database}->{tmpl_func_}->{finish}}($sth)
        if(defined($sth->{Database}->{tmpl_func_}->{finish}));
#<<-- Change
    $sth->SUPER::finish();
    return 1;
}
#>>>>> FETCH (DBD::Template::st) ----------------------------------------------------
sub FETCH ($$) {
    my ($sth, $attrib) = @_;
#NAME
    return &{$sth->{Database}->{tmpl_func_}->{name}}($sth) #<<Change (tmpl_)
                if ($attrib eq 'NAME');
#TYPE... Statement attribute
    return [(DBI::SQL_VARCHAR()) x $sth->FETCH('NUM_OF_FIELDS')]
        if($attrib eq 'TYPE');
    return [(-1) x $sth->FETCH('NUM_OF_FIELDS')]
        if($attrib eq 'PRECISION');
    return [(undef) x $sth->FETCH('NUM_OF_FIELDS')]
        if($attrib eq 'SCALE');
    return [(1) x $sth->FETCH('NUM_OF_FIELDS')]
        if($attrib eq 'NULLABLE');
    return undef if($attrib eq 'RowInCache');
    return undef if($attrib eq 'CursorName');
# Private driver attributes are lower cased
    return $sth->{$attrib} if ($attrib eq (lc $attrib));
    return $sth->SUPER::FETCH($attrib);
}
#>>>>> STORE (DBD::Template::st) ----------------------------------------------------
sub STORE ($$$) {
    my ($sth, $attrib, $value) = @_;
#1. Private driver attributes are lower cased
    if ($attrib eq (lc $attrib)) {
        $sth->{$attrib} = $value;
        return 1;
    }
    else {
        return $sth->SUPER::STORE($attrib, $value);
    }
}
#>>>>> DESTROY (DBD::Template::st) --------------------------------------------------
sub DESTROY {
    my ($sth) = @_;
    &{$sth->{Database}->{tmpl_func_}->{sth_destroy}}($sth)
        if(defined($sth->{Database}->{tmpl_func_}->{sth_destroy}));
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

DBD::Neo4p -  A DBI driver for REST::Neo4p

=head1 SYNOPSIS

    use DBI;
    $hDb = DBI->connect("dbi:Neo4p:", '', '',
        {AutoCommit => 1, RaiseError=> 1,
                tmpl_func_ => {
                    connect => \&connect,
                    prepare => \&prepare,
                    execute => \&execute,
                    fetch   => \&fetch,
                    rows    => \&rows,
                    name    => \&name,
                    table_info    => \&table_info,
                },
                tmpl_your_var => 'what you want', #...
            )
        or die "Cannot connect: " . $DBI::errstr;
    $hSt = $hDb->prepare("CREATE TABLE a (id INTEGER, name CHAR(10))")
        or die "Cannot prepare: " . $hDb->errstr();
    ...
    $hDb->disconnect();

=head1 DESCRIPTION

=head1 Functions

=head2 Driver Level

=over 4

=item datasources

=item connect

=back

=head2 Database Level
 
=over 4

=item prepare   I<(required)>

=item commit

=item rollback

=item table_info

=item disconnect

=item dbh_destroy

=item quote

=item type_info

=item funcs

=back
 
=head2 Statement Level
 
=over 4
 
=item execute   I<(required)>
 
=item fetch I<(required)>
 
=item rows  I<(required)>
 
=item name  I<(required)>
 
=item finish
 
=item sth_destroy
 
=back
 
=head1 SEE ALSO

L<REST::Neo4p>, L<REST::Neo4p::Query>.

=head1 AUTHOR

 Mark A. Jensen 
 CPAN ID : MAJENSEN
 majensen -at- cpan -dot- org

=head1 SEE ALSO

DBI, DBI::DBD

=head1 COPYRIGHT

 (c) 2013 by Mark A. Jensen

=head1 LICENSE

Copyright (c) 2013 Mark A. Jensen. This program is free software; you
can redistribute it and/or modify it under the same terms as Perl
itself.


=cut
