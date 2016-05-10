#!/usr/bin/env perl
use 5.10.1 ;
use utf8 ;
use DBI ;
use Redis ;
use JSON::XS ;
use File::Lockfile ; 
use Date::Calc::XS qw (Date_to_Time Time_to_Date Mktime);
use Encode::HanConvert; 
use POSIX qw(strftime);
use Unicode::UTF8 qw[decode_utf8 encode_utf8];
use Statistics::R;
 
binmode(STDIN, ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');
# ------------------------------------------------------------------------------------------------

my $lockfile = File::Lockfile->new('kkkk.lock' , '/tmp');
if ( my $pid = $lockfile->check ) {
        say "Seems that program is already running with PID: $pid";
        exit;
}
$lockfile->write;

my $keywordNums = 15 ;                                      # 每篇新闻取多少个keywords
my $days = 1 ;
# ----------------------------------------------
# 连接 Redis
# ----------------------------------------------
#my $redis = Redis->new();
my $redis = Redis->new(server => "10.172.107.187:6379",reconnect => 10, every => 2000);
$redis -> auth('TestDBSkst$@') ;
$redis -> select(1) ;

#=pod
# ----------------------------------------------------------------------
# 扫描新入库的文章，切词取 keywords
# ----------------------------------------------------------------------
my $dsn_dj = "DBI:mysql:database=pintimes;host=rdswufivsdsjvq31g90uipublic.mysql.rds.aliyuncs.com" ;
my $dbh_dj = DBI -> connect($dsn_dj , 'pt', 'SkstWebServer', {'RaiseError' => 1} ) ;
$dbh_dj -> do ("SET NAMES UTF8");

my $yest = strftime( "%Y-%m-%d" , localtime(time() - 86400 * $days) );          
my $sth_user = $dbh_dj -> prepare("
                                  SELECT ID,post_author,post_date,post_title,post_content
                                  FROM
                                  wp_posts
                                  WHERE
                                  post_date > '$yest 00:00:00' ") ;
$sth_user -> execute();

while (my $ref = $sth_user -> fetchrow_hashref())
{
    my $id = $ref -> {ID} ;
    next if $redis -> exists('DJ::keywords::'.$id) ;      # 已经有关键词的跳过
    
    my $post_author = $ref -> {post_author} ;
    my $post_date = $ref -> {post_date} ;
  
    my $title = decode_utf8 $ref -> {post_title} ;
    my $content = trad_to_simp decode_utf8 $ref -> {post_content} ;

    # 去除一些爬虫留下的杂质，当然这里情况很多，以后如果有新的新闻来源页面出现新的奇怪的东西，还需要额外加逻辑
    $content =~ s/<a[^<>]*?<\/a>//g ;
    $content =~ s/<[^<>]*?>//g ;
    $content =~ s/[\r\n\s]//g ;
    $content =~ s/&?[a-zA-Z0-9]+;//g ;
    
    # 这里先把内容写入临时文本再让R去加载
    open my $fh_temp , ">:utf8" , '/tmp/temp.txt' ;
    # 这里把文章标题也写进去，因为部分文章仅仅只是充满图片
    print $fh_temp $title."\t".$content ;               
    
    my (@nums,@names) ;
    eval
    {
        # 这里显式地配置R路径，保证crontab有效
        #my $R = Statistics::R -> new(bin => '/usr/local/bin/R');
        my $R = Statistics::R -> new(bin => '/usr/bin/R');	
        my $output = $R -> run
        (
            q`library(jiebaR)` ,
            "keys = worker('keywords', topn = $keywordNums ,encoding = 'UTF-8')" ,
            "keys <= '/tmp/temp.txt'"
        );
        
        # 这里因为是获取R编译器的STDOUT，所以字符串处理一下
        while($output =~ /([0-9\.]+)[\s\n]/g) {
            push @nums , $1 ;
        }
        while($output =~ /"(.*?)"[\s\n]/g) {
            push @names ,decode_utf8 $1 ;
        }
        #$keywords = join ";" , map {s/^"|"$//g;$_} grep {/"/} split " " , $output ;
        $R->stop();
    };
    next unless scalar @nums ;
    my %keywords ;
    for( 1 .. $keywordNums){
        my $sub = $_ - 1 ;
        $keywords{$names[$sub]} = $nums[$sub] if $names[$sub];
    }
    my $keywords_info = encode_json \%keywords;
    insert_redis_scalar('DJ::keywords::'.$id , $keywords_info) if $keywords_info ;
    insert_redis_scalar('DJ::time::' . $id  ,  $post_date) ;
    
    # 下面这个是休息时间，看运行环境的硬件配置吧，如果CPU太渣了，就慢点弄
    #select(undef, undef, undef, 0.15);                    
}
#=cut
#=pod
# ----------------------------------------------------------------
# 扫描相关文章
# ----------------------------------------------------------------
foreach($redis->keys( "DJ::keywords::*" ) )
{
    my $k = $_ ;
    my ($id) = $k =~ /::(\d+)$/ ;
    
    # 如果需要重新初始化所有历史文章的推荐结果，注释掉下面的 next 逻辑
    my $id_flag = $redis -> get('DJ::simi_id');
    next if $id < ($id_flag - 3) ;                      
    next if $redis->scard( 'DJ::related3::'.$id ) ;                     # 如果已有推荐结果，跳过
    
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
        next if $id2 < $id - 3300 ;
        
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
    
    $redis -> set('DJ::simi_id' , $id) if $id > $id_flag ;
}
#=cut

$lockfile->remove;

# =========================================  functions  ==========================================

sub insert_redis_scalar
{
    my ($rediskey,$redisvalue) = @_ ;
    $redis->set($rediskey,$redisvalue);
    say $rediskey . "\t=>\t" .decode_utf8 $redisvalue ;
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
