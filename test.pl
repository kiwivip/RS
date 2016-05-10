#!/usr/bin/env perl
use 5.18.2;
use utf8 ;
use Redis;
use DBI;
use Encode;
use POSIX qw(strftime);
#use Unicode::UTF8 qw[decode_utf8 encode_utf8];

#my $redis = Redis->new(server => "192.168.1.178:6379",reconnect => 10, every => 2000);
#$redis -> auth('TestDBSkst$@') ;
#$redis -> select(1) ;
my $t = strftime( "%H" , localtime(time()-37000)) ;
say $t ;

=pod
my $redis = Redis->new(server => "192.168.199.55:6379",reconnect => 10, every => 2000);
#$redis -> auth('TestDBSkst$@') ;
#$redis -> select(2) ;
foreach($redis->keys( "CBS*_TIME" ) )
{
    my $k = $_ ;
    #my $n = $redis->zcard($k) ;
    #my $v = $redis -> get( $k ) ;
    $redis -> del($k) ;
    say "$k" ;
}

