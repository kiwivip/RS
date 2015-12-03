#!/usr/bin/env perl 
use 5.10.1 ;
use utf8 ;
use Redis;
use DBI;
use Encode;
use Unicode::UTF8 qw[decode_utf8 encode_utf8];

my $redis = Redis->new(server => "10.172.107.187:6379",reconnect => 10, every => 2000);
$redis -> auth('TestDBSkst$@') ;
$redis -> select(1) ;

open my $fh , "<:utf8" , 'txt' ;
while(<$fh>){
	chomp ;
	my ($k,$v) = split "\t" , $_ ;
	say "$k => $v" ;
	$redis -> set($k,encode_utf8 $v) ;	

}
=pod
my $redis = Redis->new(server => "10.172.107.187:6379",reconnect => 10, every => 2000);
$redis -> auth('TestDBSkst$@') ;
$redis -> select(1) ;
foreach($redis->keys( "DJ::keywords*" ) )
{
    my $k = $_ ;
    #my $n = $redis->zcard($k) ;
    my $v = $redis -> get( $k ) ;
    #    #$redis -> del($k) ;
    say "$k\t$v" ;
}

