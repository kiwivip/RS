#!/usr/bin/env perl
use 5.18.2;
use utf8 ;
use Redis;
use DBI;
use Encode;
use POSIX qw(strftime);
#use Unicode::UTF8 qw[decode_utf8 encode_utf8];

my $redis = Redis->new(server => "192.168.199.24:6379",reconnect => 10, every => 2000);

my $dsn_new  = "DBI:mysql:database=cbs_content;host=192.168.199.88;port=3307" ;
my $dbh_new = DBI -> connect($dsn_new, 'recommenduser' , 'recommendServer!+!', {'RaiseError' => 1} ) ;
$dbh_new -> do ("SET NAMES UTF8");
#$redis -> auth('TestDBSkst$@') ;
$redis -> select(2) ;
my @sss = $redis->smembers( "CBS::payin::uv_2016-05-08" );
#say join "\n" , @sss ;
for(@sss){
    my $id = $_ ;
    my $ref_account = get_user_from_accountId($dbh_new , $id) ;
    my ($l99NO,$gender,$name) = ($ref_account->{l99NO} , $ref_account->{gender} , $ref_account->{name}) ;
    say $id."\t".$l99NO ;

}



sub get_user_from_accountId
{
	my ($dbh,$accountId) = @_ ;
	my $ref_accountId ;
	my $sth_account = $dbh -> prepare(" SELECT userId,userNO,userName,gender,status FROM cbs_user WHERE userId = $accountId ") ;
	$sth_account -> execute();
	while (my $ref_account = $sth_account -> fetchrow_hashref())
	{
		my $accountId = $ref_account -> {userId};
		my $l99NO     = $ref_account -> {userNO};
		my $name      = $ref_account -> {userName};
		my $gender    = $ref_account -> {gender} ;
		my $status    = $ref_account -> {status} ;
		$ref_accountId -> {l99NO}  = $l99NO ;
		$ref_accountId -> {name}   = $name ;
		$ref_accountId -> {gender} = $gender ;
		$ref_accountId -> {status} = $status ;
	}
	$sth_account -> finish ;
	return $ref_accountId ;
}

 
#my $t = strftime( "%H" , localtime(time()-37000)) ;
#say $t ;

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

