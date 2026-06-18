#
# Guts of the parsing stolen from Net::Netrc at cpan.
#

use FileHandle;
use Data::Dumper;

use strict;

sub parse_netrc
{
    my($file) = @_;

    my %netrc;
    
    my $re = get_re();
    my $fh;
    if ($fh = FileHandle->new($file, "r")) {
	my ($mach, $macdef, $tok, @tok) = (0, 0);
	
	while (<$fh>) {
	    undef $macdef if /\A\n\Z/;
	    
	    if ($macdef) {
		push(@$macdef, $_);
		next;
	    }
	    
	    s/^\s*//;
	    chomp;
	    
	    while (length && s/$re//) { 
		(my $tok = $+) =~ s/\\(.)/$1/g;
		push(@tok, $tok);
	    }
	    
	TOKEN:
	    while (@tok) {
		if ($tok[0] eq "default") {
		    shift(@tok);
		    $mach = {};
		    $netrc{default} = [$mach];
		    
		    next TOKEN;
		}
		
		last TOKEN
		    unless @tok > 1;
		
		$tok = shift(@tok);
		
		if ($tok eq "machine") {
		    my $host = shift @tok;
		    $mach = {machine => $host};
		    
		    $netrc{$host} = []
			unless exists($netrc{$host});
		    push(@{$netrc{$host}}, $mach);
		}
		elsif ($tok =~ /^(login|password|account)$/) {
		    next TOKEN unless $mach;
		    my $value = shift @tok;
		    
		    # Following line added by rmerrell to remove '/' escape char in .netrc
		    $value =~ s/\/\\/\\/g;
		    $mach->{$1} = $value;
		}
		elsif ($tok eq "macdef") {
		    next TOKEN unless $mach;
		    my $value = shift @tok;
		    $mach->{macdef} = {}
		    unless exists $mach->{macdef};
		    $macdef = $mach->{machdef}{$value} = [];
		}
	    }
	}
	$fh->close();
    }
    
    return \%netrc;
}


sub  get_re {
    return qr/^("((?:[^"]+|\\.)*)"|((?:[^\\\s]+|\\.)*))\s*/
}

1;
