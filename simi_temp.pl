#!/usr/bin/env perl
use 5.10.1 ;
use utf8 ;
use DBI ;
use Redis ;
use JSON::XS ;
use Date::Calc::XS qw (Date_to_Time Time_to_Date Mktime);
use Encode::HanConvert; 
use POSIX qw(strftime);
use Unicode::UTF8 qw[decode_utf8 encode_utf8];
use Statistics::R;
 
binmode(STDIN, ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');
# ------------------------------------------------------------------------------------------------

my $redis = Redis->new(server => "10.172.107.187:6379",reconnect => 10, every => 2000);
$redis -> auth('TestDBSkst$@') ;
$redis -> select(1) ;
my $id_flag = 133333 ;
foreach($redis->keys( "DJ::keywords::*" ) )
{
    my $k = $_ ;
    my ($id) = $k =~ /::(\d+)$/ ;
    
    next if $id > $id_flag ;                      
    next if $redis->scard( 'DJ::related3::' .$id ) ;                     # 如果已有推荐结果，跳过
    next if $redis->scard( 'DJ::related10::'.$id ) ; 
    my $ref_words = decode_json $redis -> get( $k ) ;
    # 这里取权重超过20的关键词，当然这个是根据测试的结果目测的，后续如果读家的文章越来越长，这个值有待改变
    my @words = grep {$$ref_words{$_} > 20} keys %$ref_words ;              
    next unless scalar(@words) > 1 ;    
    say $k . "\t" ."@words" ;

    foreach($redis->keys( "DJ::keywords::*" ) )
    {
        my $k2 = $_ ;
        # 自己和自己就不比了
        next if $k2 eq $k ;
        
        my ($id2) = $k2 =~ /::(\d+)$/ ;
        
        
        my $ref2 = decode_json $redis -> get( $k2 ) ;
        my @words2 = grep {$$ref2{$_} > 20} keys %$ref2 ;
        
        my %d ;
        foreach (@words)  { $d{$_} = 1 ; }
        foreach (@words2) { $d{$_} = 1 ; }
        
        my $time1 = maketime($redis->get('DJ::time::' . $id)  ) ;
        my $time2 = maketime($redis->get('DJ::time::' . $id2) ) ;
            
        # 两组关键词(只看权重大于20的)交集个数
        my $num_word1 = scalar(@words) ;
        my $num_word2 = scalar(@words2) ;
        my $num_d     = scalar(keys %d) ;
        my $num_simi = $num_word1 + $num_word2 - $num_d ;
        my $relatedNum ;
        # ------------------------------------------------------------------
        # 下面，就是针对交集情况判定相关性了
        # 事实上，这里我们先没有考虑文章'重复'的情况
        # ------------------------------------------------------------------
        if ($num_d > 0 && $num_d == $num_word1 && $num_d == $num_word2)
        {
            say "\tD\t" . $id2 . "\t" ."@words2" ;
            $relatedNum = 10 ;
        }
        else
        {
            # 4~11  这2篇文章我们认定为 '强相关'
            if ($num_simi > 3 and $num_simi < 12){
                say "\tS+\t" . $id2 . "\t" ."@words2" ;
                $relatedNum = 2 ;
            }
            # 2~3   这2篇文章我们认定为 '弱相关'
            elsif($num_simi > 1 and $num_simi < 4){
                say "\tS-\t" . $id2 . "\t" ."@words2" ;
                $relatedNum = 3 ;
            }
            # 12个以上  这2篇文章我们认为 '重复'
            elsif($num_simi > 11 ){
                say "\tD\t" . $id2 . "\t" ."@words2" ;
                $relatedNum = 10 ;
            }
            else{
                next ;
            }
        }
        
        $redis -> sadd( 'DJ::related' . $relatedNum . '::' . $id   , $id2 ) ;
        $redis -> sadd( 'DJ::related' . $relatedNum . '::' . $id2  , $id ) ;
        
        my $num_2 = $redis->scard('DJ::related2::'.$id2) ;
        if ($num_2 > 100)
        {
            my $ref_members = $redis -> smembers('DJ::related2::'.$id2) ;
            my $min = get_min($ref_members) ;
            $redis->srem('DJ::related2::'.$id2 , $min) ;
        }
        
        #$redis_test -> zadd( 'DJ::related' . $relatedNum . '::' . $id  , $time2 , $id2 ) if $time2;
        #$redis_test -> zadd( 'DJ::related' . $relatedNum . '::' . $id2 , $time1 , $id  ) if $time1;
        
    }
    #$redis -> set('DJ::simi_id' , $id) if $id > $id_flag ;
}
#=cut

# =========================================  functions  ==========================================

sub insert_redis_scalar
{
    my ($rediskey,$redisvalue) = @_ ;
    $redis->set($rediskey,$redisvalue);
    say "$rediskey => $redisvalue" ;
}

sub maketime
{
    my ($time) = @_ ;
    my ($year,$month,$day,$hour,$min,$sec) = $time =~ /^(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)$/ ;
    my @s ;
    eval{@s = Mktime($year,$month,$day, $hour,$min,$sec)};
    return $s[0] ;
}

sub get_min
{
    my ($ref_list) = @_ ;
    my @list = @$ref_list ;
    my $min = $list[0];
    foreach my $i (@list){
	    $min = $i if ($i < $min);
    }
    return $min;
}
