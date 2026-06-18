#
# Benchmark a node
#
# Usage:
#
#  benchmark.pl collection node
#
# We look up the shards of the given collection on the given node and
# run queries:
#

use lib '/home/olson/BV-BRC/solr_tools/lib';
use strict;
use Solr;
use Getopt::Long::Descriptive;

my($opt, $usage) = describe_options("%c %o collection node",
				    ["help|h" => "Show this help message"]);
print($usage->text), exit 0 if $opt->help;
die($usage->text) if @ARGV != 2;

my $col = shift;
my $node = shift;

my $solr = Solr->new("https://$node:15183/solr");

$solr->distributed_query("genome_feature", "q=product:trna&sort=feature_id asc", "/dev/shm/bob",
		     { node => $node,
			   all_shards => 1,
			   #max_shards => 4,
			   discard_output => 1,
		       });
