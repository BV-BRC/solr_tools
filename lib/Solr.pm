package Solr;

#
# Some common solr utilities for querying/dumping data.
#

use strict;
use JSON::XS;
use CBOR::XS;
use LWP::UserAgent;
use IO::Socket::SSL;
use File::Slurp;
use Data::Dumper;
use base 'Class::Accessor';
use EV;
use URI;
use AnyEvent;
use AnyEvent::Util qw(fork_call);
use Furl;
use Time::HiRes 'gettimeofday';
use Cache::Memory;
use Cache::File;
use Netrc;
use File::Basename;

__PACKAGE__->mk_accessors(qw(ua url json cache username password));

our $cluster_base = "/solr/cluster";
our $hostname = `hostname -f`;
chomp $hostname;

#
# Read the folders under $cluster_base to find the active cluster node configs.
sub cluster_nodes
{
    my @res;
    for my $f (sort { $a cmp $b } <$cluster_base/*/PORT-*>)
    {
	my $dir = dirname($f);
	my($port) = $f =~ /PORT-(\d+)$/;
	my $stopped = -f "$dir/STOPPED";
	my $pid = read_file("$dir/logs/solr-$port.pid", err_mode => 'quiet');
	chomp $pid;

	my $pid_status = 'bad';
	my $args = read_file("/proc/$pid/cmdline", err_mode => 'quiet');
	if ($args)
	{
	    my @args = split(/\0/, $args);
	    for my $arg (@args)
	    {
		if ($arg =~ /jetty.port=(\d+)/)
		{
		    if ($1 eq $port)
		    {
			$pid_status = 'live';
			last;
		    }
		}
	    }
	}
	else
	{
	    $pid_status = 'dead';
	}
	
	push @res, {
	    dir => $dir,
	    port => $port,
	    stopped => ($stopped ? 1 : 0),
	    pid => $pid,
	    pid_status => $pid_status,
	    url => "https://$hostname:$port/solr",
	};
    }
    return @res;
}

#
# Credentials is [realm, username, password]
#

sub new
{
    my($class, $url, $opts) = @_;

    my $self = {
	url => $url,
	json => JSON::XS->new->pretty(1),
	cache => Cache::File->new(cache_root => '/dev/shm/cache.' . $ENV{USER},
				  default_expires => '60 sec'),
#	cache => Cache::Memory->new(namespace => 'Solr',
#				    default_expires => '60 sec'),
    };

    my $uri = URI->new($url);

    #
    # Manage credentials
    #
    my $realm = 'solr';
    my($user, $pass);
    if ($opts->{credentials})
    {
	($realm, $user, $pass) = @{$opts->{credentials}};
    }
    else
    {
	my $netrc_file = $opts->{netrc} // $ENV{NETRC};
	if ($netrc_file)
	{
	    my $host = $uri->host;
	    my $netrc = parse_netrc($netrc_file);
	    my $ent = $netrc->{$host}->[0];
	    $ent or die "Cannot find creds for $host\n";
	    $user = $ent->{login};
	    $pass = $ent->{password};
	}
	else
	{
	    die "No credentials provided\n";
	}
    }

    my $ua = LWP::UserAgent->new(
      ssl_opts => {
	  verify_hostname => 0,
	  SSL_verify_mode => IO::Socket::SSL::SSL_VERIFY_NONE,
       });

    $ua->credentials($uri->authority, $realm, $user, $pass);
    $ua->ssl_opts(verify_hostname => 0);

    $self->{username} = ($user);
    $self->{password} = ($pass);

    if ($uri->userinfo() eq '')
    {
	$uri->userinfo("$user:$pass");
	$self->{url} = "$uri";
    }

    $self->{ua} = $ua;

    return bless $self, $class;
}

sub shard_count
{
    my($self, $collection) = @_;
    my $status = $self->cluster_status();

    my $sdata = $status->{collections}->{$collection};
    return scalar keys%{$sdata->{shards}};
}

sub cluster_status
{
    my($self) = @_;

    my $status = $self->cache->entry('cluster_status')->thaw();
    if (!$status)
    {
	$status = $self->get_cluster_status();
	$self->cache->entry('cluster_status')->freeze($status);
    }
    return $status;
}

sub get_collection_count
{
    my($self, $col, @filters) = @_;

    my $filters = join("&", map { "fq=$_" } @filters);

    my $res = $self->query($col, "q=*:*&rows=0&$filters");

    return $res->{numFound};
}

sub ping
{
    my($self, $col) = @_;
    my $url = $self->url . "/$col/admin/ping";

    my $res = $self->ua->get($url);
    if ($res->is_success)
    {
	my $dat = decode_json($res->content);
	return $dat->{status};
    }
    else
    {
	return 'ERROR ' . $res->code;
    }
}

sub query
{
    my($self, $col, $qry) = @_;

    my $url = $self->url . "/$col/select";
    my $furl = Furl::HTTP->new(headers => ['Content-Type' => 'application/x-www-form-urlencoded'],
			       timeout => 180,
			       bufsize => 40_000,
			       ssl_opts => {
				   verify_hostname => 0,
				   SSL_verify_mode => IO::Socket::SSL::SSL_VERIFY_NONE,
			       }
			      );

    my($proto, $code, $msg, $hdrs, $body) = $furl->post($url, [], $qry);
    if ($code ne 200)
    {
	die Dumper($url, $qry, $proto, $code, $msg, $hdrs, $body);
    }

    my $doc = $self->json->decode($body);
    my $resp = $doc->{response};
    if (wantarray)
    {
	return ($resp, $doc);
    }
    else
    {
	return $resp;
    }
}

#
# Perform a query distributed across the shard for the given collection.
#
# Option keys:
#
# n_processes - max processes used to run queries. Defaults to 40
# as_cbor     - true if we wish to write CBOR instead of JSON
#

sub distributed_query
{
    my($self, $col, $params, $out_base, $opts) = @_;

    my $shards = $opts->{all_shards} ? $self->get_all_shards_for_collection($col) :  $self->get_shards_for_collection($col);
    my @queries;

    my $cbor = CBOR::XS->new->text_strings(1);

    my @parts = split(/&/, $params);
    my ($fl) = grep { s/^fl=// } @parts;
    my @fields = split(/,/, $fl);

    my $userinfo = URI->new($self->url)->userinfo;

    if (my $node = $opts->{node})
    {
	print Dumper($node, $shards);
	my @shards = grep { $_->[1]->{node_name} =~ /^$node/ } @$shards;
	if ($opts->{max_shards} && @shards > $opts->{max_shards})
	{
	    $#shards = $opts->{max_shards} - 1;
	}
	@$shards = @shards;
    }
    print Dumper($shards);

    for my $s (@$shards)
    {
	my($sname, $rep) = @$s;

	if ($opts->{select_shards})
	{
	    next unless $opts->{select_shards}->{$sname};
	}
	    
	my $url = "$rep->{base_url}/$col/select";
	# Inject base authentication information into the shard URL we looked up
	my $uri = URI->new($url);
	$uri->userinfo($userinfo);
	my $q = join("&",
		     $params,
		     "shards=$sname",
		     "preferLocalShards=true",
		    );
	push(@queries, [$sname, $uri, $q]);
    }

    my $furl = Furl::HTTP->new(headers => ['Content-Type' => 'application/x-www-form-urlencoded'],
			       timeout => 180,
			       bufsize => 400_000,
			       ssl_opts => {
				   verify_hostname => 0,
				   SSL_verify_mode => IO::Socket::SSL::SSL_VERIFY_NONE,
			       });


    my @out;

    $AnyEvent::Util::MAX_FORKS = $opts->{n_processes} // 40;

    my $total = 0;
    my $cv = AE::cv;
    for my $q (@queries)
    {
	$cv->begin;
	fork_call {
	    
	    my($shard, $url, $base_qry) = @$q;
	    
	    my $mark = "*";
	    
	    my $rows = 25000;
	    
	    my $tot = 0;
	    my $done;
	    my @out;
	    my $chunk = 1;
	    print STDERR "$shard $url\n";
	    while (!$done)
	    {
		my $qry = join("&", $base_qry,
			       "rows=$rows",
			       "cursorMark=$mark");

		$qry =~ s/ /+/g;


		my $retries = 10;
		my $t_elap;
		my($proto, $code, $msg, $hdrs, $body);
		while ($retries)
		{
		    # die Dumper($url, $qry);
		    my $t_start = gettimeofday;
		    ($proto, $code, $msg, $hdrs, $body) = $furl->post($url, [], $qry);
		    my $t_end = gettimeofday;
		    $t_elap = $t_end - $t_start;
		    if ($code eq '500' && $msg =~ /Internal Response: Cannot read.* timeout/)
		    {
			print STDERR "Sleep & retry on timeout\n";
			sleep 1;
			$retries--;
		    }
		    elsif ($code ne '200')
		    {
			die Dumper($url, $qry, $proto, $code, $msg, $hdrs, $body);
		    }
		    else
		    {
			last;
		    }
		}

		my $doc = $self->json->decode($body);
		my $resp = $doc->{response};
		my $docs = $resp->{docs};
		my $ndocs = @$docs;
		$tot += $ndocs;

		my $t_rate = $t_elap > 0 ? sprintf("%.2f MB/s", length($body) / $t_elap / 1e6) : "";
		my $next_mark = $doc->{nextCursorMark};
		print STDERR sprintf "$shard $code $resp->{start} $resp->{numFound} $ndocs $tot %.03f $t_rate\n", $t_elap;
		$done = 1 if $next_mark eq $mark;
		$mark = $next_mark;

		if (@$docs > 0)
		{
		    my $suffix = $opts->{as_cbor} ? "cbor" : "json";
		    my $out_file = sprintf "$out_base.$shard.%04d.$suffix", $chunk;
		    if ($opts->{discard_output})
		    {
		    }
		    elsif ($opts->{as_cbor})
		    {
			write_file($out_file, $cbor->encode($docs));
		    }
		    elsif ($opts->{as_text})
		    {
			open(my $fh, ">", $out_file) or die "Cannot open $out_file: $!";
			print $fh join("\t", @$_{@fields}), "\n" foreach @$docs;
		    }
		    else
		    {
			write_file($out_file, $self->json->encode($docs));
		    }
		}
		# $out .= join("", map { $self->json->encode($_) } @$docs);

		$chunk++;
	    }
	    print STDERR "$shard $tot\n";
	    return ($tot, $shard);
	}
	sub {
	    my($shard_total, $shard) = @_;
	    if (!defined($shard_total))
	    {
		warn "ERROR @$q: $@";
	    }
	    else
	    {
		print STDERR "Shard total $shard_total\n";
		$total += $shard_total;
	    }
	    $cv->end;
	}
    }
    print STDERR "Wait\n";
    $cv->recv;
    return $total;
}

#
# Pick a set of shard replicas to query across the given collection.
# Prefer replicas that are not leaders.
#

sub get_shards_for_collection
{
    my($self, $col) = @_;

    my $cstat = $self->cluster_status($col);
    my $col_info = $cstat->{collections}->{$col};

    my @out;
    while (my($shard, $sdat) = each %{$col_info->{shards}})
    {
	my @reps = sort { $a->{leader} cmp $b->{leader} } grep { $_->{state} = 'active' } values %{$sdat->{replicas}};
	my $rep = $reps[0];
	push(@out, [$shard, $rep]);
    }
    return \@out;
}

sub get_all_shards_for_collection
{
    my($self, $col) = @_;

    my $cstat = $self->cluster_status($col);
    my $col_info = $cstat->{collections}->{$col};

    my @out;
    while (my($shard, $sdat) = each %{$col_info->{shards}})
    {
	my @reps = sort { $a->{leader} cmp $b->{leader} } grep { $_->{state} = 'active' } values %{$sdat->{replicas}};
	for my $rep (@reps)
	{
	    push(@out, [$shard, $rep]);
	}
    }
    return \@out;
}

sub get_shards_for_node
{
    my($self, $node) = @_;
    my @out;
    my $stat = $self->cluster_status();
    while (my($col, $cdata) = each %{$stat->{collections}})
    {
	my $shards = $cdata->{shards};
	while (my($shard, $sdata) = each %$shards)
	{
	    while (my($rep, $repdata) = each %{$sdata->{replicas}})
	    {
		if ($repdata->{base_url} eq $node)
		{
		    push(@out, [$col, $shard, $sdata->{health}, $rep, $repdata]);
		}
	    }
	}
    }
    return \@out;
}

#
# Retreive cluster status.
#
# This is the data returned by the Solr collections api:
# https://solr.apache.org/guide/8_8/cluster-node-management.html#clusterstatus-response
#
sub get_cluster_status
{
    my($self) = @_;

    my $res = $self->ua->get($self->url . "/admin/collections?action=clusterstatus");
    if (!$res->is_success)
    {
	die "Clusterstatus failed: " . $res->content;
    }

    my $dat = $self->json->decode($res->content);

    return $dat->{cluster};
}

sub get_schema
{
    my($self, $core) = @_;
    my $url = $self->url . "/$core/schema";
    my $res = $self->ua->get($url,
			     Accept => "*/*");
    if (!$res->is_success)
    {
	die "Error retrieving $url: " . $res->content;
    }

    my $schema = $self->json->decode($res->content);
    return $schema;
}

sub list_collections
{
    my($self, $core) = @_;
    my $url = $self->url . "/admin/collections?action=LIST";

    my $res = $self->ua->get($url,
			     Accept => "*/*");
    if (!$res->is_success)
    {
	die "Error retrieving $url: " . $res->content;
    }

    my $list = $self->json->decode($res->content);
    return $list;
}

#
# Get the params needed for distributed queries for the collection.
# Returns (unique_key, field-list)
#
sub get_collection_query_params
{
    my($self, $col) = @_;

    if (!$self->{param_cache}->{$col})
    {
	my $schema = $self->get_schema($col);
	
	my $unique_key = $schema->{schema}->{uniqueKey};
	
	my @fields = grep { $_->{stored} } @{$schema->{schema}->{fields}};
	my @field_names = map { $_->{name} } @fields;
	
	@field_names = grep { $_ ne "_version_" } @field_names;

	$self->{param_cache}->{$col} = [$unique_key, \@field_names];
    }
    return @{$self->{param_cache}->{$col}};
}

#
# Push a set of CBOR-formatted files through the indexer.
#

sub index_cbor_set
{
    my($self, $collection, $files, $do_commit) = @_;

    my $cbor = CBOR::XS->new->text_strings(1);

    for my $i (0..$#$files)
    {
	my $file = $files->[$i];
	my $commit_this;
	print "$collection $file $commit_this\n";
	my $url = "$self->{url}/$collection/update/cbor";
	if ($i == $#$files && $do_commit)
	{
	    $url .= "?commit=true";
	}
	print "$url\n";
	my $data = read_file($file);

	# my $tmp = decode_cbor($data);
	# for my $ent (@$tmp)
	# {
	#     if (exists $ent->{public})
	#     {
	# 	$ent->{public} = $ent->{public} ? 1 : 0;
	#     }
	# }

	print "$url\n";
	my $res = $self->ua->post($url,
				  'Content-Type' => "application/cbor",
				  Content => $data);
	if (!$res->is_success)
	{
	    die "Error loading $file: " . $res->content;
	}
	print $res->content;
    }
    
}
    


    
1;
