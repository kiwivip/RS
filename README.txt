项目工程说明文档

项目下文件功能说明
        －rs_1214.pl                         ＃海外头条相关新闻推荐系统的工程，计划任务中周期运行
                                             ＃功能：生成相关新闻的ids写入redis供api提取
        －kpi_0516.pl                        ＃在终端打印出数据月报所需的相关数据
        －kpi_keywords.pl                    ＃生成kpi月期间新闻关键词列表的工程（月报工程）
        －words_YYYY-MM.txt                  ＃月报工程的生成文件，描述kpi月期间对所有新闻正文内容分析得到的关键词列表，按tf/idf权重
                                             ＃其中第二列的权重值同时提供给R绘制词云图时作为词语颜色判定的依据
        －googlestr_YYYY-MM.txt              ＃月报工程的生成文件，描述kpi月期间用户使用搜索的关键词列表，按次数权重
        
其它说明
通过R绘制词云图的代码因为极短且要实际调整所以没有固化，请单独手动在IDE（RStudio）中执行生成图例

具体代码（手动逐行执行）：
library(wordcloud)                                                                  ＃这个包是绘图功能主体
library(showtext)                                                                   ＃这个包是为了解决图中的中文字体的兼容
showtext.auto()
data_word = read.table('/YourPathTo/words_2016-05.txt',encoding = 'UTF-8',sep = "\t")
mycolor <- colorRampPalette(c('blue','red'))(800)                                   ＃这里的800要根据关键词的权重值具体调整，一般取max，目的是为了最后出来的图颜色更好看
jpeg(filename = '/YourPathTo/YourName.jpg',width = 800,height = 800,units = 'px')   ＃这里的800也是要根据实际出来的图效果来调整，目的是为了最后的图好看     
wordcloud(data_word$V1,data_word$V2,c(6,1),random.order = FALSE,colors = mycolor)   ＃这里的6效果同上
dev.off()                                                                           ＃这句话是说不用在IDE中画了直接生成jpg文件
   
本地绘图时请自行安装依赖的2个library
另外，相关新闻工程rs_1214.pl，是perl与R的混编工程，会依赖R的jiebaR包，如果迁移至新服务器则需要手动配置环境。

        
亚马逊云服务器上的crontab备份：
*/2 * * * * perl /home/RS/rs_1214.pl  >>/home/RS/rs.log &
1 8,10,12,16 * * *   source /etc/profile ; perl /home/RS/scpLogs4dj.pl >>/home/RS/scp.log &
30 8,10,12,15,18,20 * * * perl /home/RS/DJ_20160515.pl  >>/home/RS/dj_day.log &

