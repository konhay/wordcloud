import pymysql
import pandas as pd
import jieba
import jieba.posseg as pseg
import jieba.analyse
import re
import random
from operator import itemgetter
from pytagcloud import create_tag_image, make_tags, LAYOUT_HORIZONTAL

stopwords_filepath = 'stopwords.txt'
cust_stopwords = ['微博', '分享', '图片', '圖片', '客户端', '真的', '感觉', 'Share', 'image', 'fuck', 'Weico', 'iPhone']

def clean_content_cust(content):
    '''专为微博爬虫客制化的清理方法，清理单条微博内容'''
    start = 0

    #去掉//后面的全部内容
    p = content.find('//', start)
    content = content[:p]

    #去掉【后面的全部内容
    p = content.find('【', start)
    content = content[:p]

    #去掉[表情]
    def clean_content_e(content):
        start = 0
        pa = content.find('[', start)
        pb = content.find(']', start)
        if (start <= pa):
            content = content[:pa] + content[pb + 1:]
            # 递归去掉多个表情
            return clean_content_e(content)
        return content
    content = clean_content_e(content)

    #去掉末尾的地点如:合肥·三十头镇
    p = content.find('·', start)
    if p >0 and content[p]:
        while content[p] != ' ':
            p = p-1
        content = content[:p]

    #去掉末尾的地点如:我在:铜陵北路
    p = content.find('我在:', start)
    content = content[:p]

    #去掉包含“分享自”的微博
    p = content.find('分享自', start)
    if p > 0:
        content = content[:0]

    #去掉包含“新浪微博”的微博
    p = content.find('新浪微博', start)
    if p >= 0:
        content = content[:0]

    #去掉包含“点击查看”的微博
    p = content.find('点击查看:', start)
    if p >= 0:
        content = content[:0]

    #去掉包含“微人脉”的微博
    p = content.find('微人脉', start)
    if p >= 0:
        content = content[:0]

    #去掉包含“点击查看”的微博
    p = content.find('微博客户端', start)
    if p >= 0:
        content = content[:0]

    #去掉包含“微博等级”的微博
    p = content.find('微博等级', start)
    if p >= 0:
        content = content[:0]

    return content


def remove_stopwords(seg_list):
    '''去除分词集合中的停止词'''

    # 初始化默认停止词
    stopwords = list([ line.strip().decode('gbk') for line in open(stopwords_filepath) ])

    # 追加自定义停止词
    for i in range(len(cust_stopwords)):
        stopwords.append(cust_stopwords[i])

    seg_list_nstop = []
    for seg in seg_list:
        if seg[0] not in stopwords:
            seg_list_nstop.append(seg)
    return seg_list_nstop


def judge_stopwords(seg):
    '''判断分词是否为停止词'''

    # 初始化默认停止词
    stopwords = list([ line.strip().decode('gbk') for line in open(stopwords_filepath) ])

    # 追加自定义停止词
    for i in range(len(cust_stopwords)):
        stopwords.append(cust_stopwords[i])

    if seg not in stopwords:
        return True
    else:
        return False


def get_content_str(sql):
    '''从数据库获取内容串'''
    # reload(sys)
    # sys.getdefaultencoding()#ascii'
    # sys.setdefaultencoding('utf8')
    conn = pymysql.connect(host='127.0.0.1',
                           # unix_socket='/tmp/mysql.sock',
                           user='root',
                           passwd='root',
                           db='tweetinfo',
                           charset='utf8')#防止控制台打印中文乱码
    cur = conn.cursor()
    cur.execute(sql)
    rowcount = cur.rowcount
    # contents = []
    content_str = ""
    nid = 0
    while nid < rowcount:
        nid += 1
        content = clean_content_cust(cur.fetchone()[0])
        print(str(nid) + '.' + content)
        content_str += content
        # contents.append(content)

    # 去除地名
    # contents = cpca.transform(contents)['地址']
    # for i in range(len(contents)):
    #     content_str += contents[i]

    # 去掉标点符号和数字
    content_str = re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？?、~@#￥%……&*（）「」：～《★》0123456789]+".decode("utf8"), "", content_str)

    cur.close()
    conn.close()

    return content_str


def cut_all(content_str):
    '''预留方法，jieba分词精确模式'''
    seg_list_ca = jieba.cut(content_str, cut_all=False)
    seg_list_ca = remove_stopwords(seg_list_ca)
    #print(",".join(seg_list_ca))
    return seg_list_ca


def cut_flag(content_str):
    '''预留方法，jieba分词词性模式'''
    seg_list_flag = pseg.cut(content_str)
    l = []
    for word, flag in seg_list_flag:
        if judge_stopwords(word):
            l.append((word, flag, 1))
    df = pd.DataFrame(l, columns=('word','flag','count'))
    #print df.groupby('flag').sum()
    #print df[df.flag.str.contains(r'^n.*?')][word] #以n开头的都是名词
    return df


def content_jieba(content_str, mode):
    '''
    用jieba分词处理内容串
    '''
    # TF-IDF
    if mode == 'TF-IDF' :
        print ('-----------------------------------------------')
        l3 = jieba.analyse.extract_tags(content_str,
            topK=20,
            withWeight=True,
            allowPOS=())

    # TextRank
    if mode == 'TextRank' :
        print ('-----------------------------------------------')
        l3 = jieba.analyse.textrank(content_str,
            topK=20,
            withWeight=True,
            allowPOS=('ns', 'n', 'vn', 'v', 'nr', 'nrt', 'nt', 'nz'))

    for l in remove_stopwords(l3):
        print (l[0] + ':' + str(l[1])).encode('utf8')

    return l3


def make_tag_image(content_jieba): 
    '''使用pytagcloud对分词结果进行词云绘图'''
    words={}
    for tag in remove_stopwords(content_jieba):
        words[tag[0]] = float(tag[1])
    sorted_words = sorted(words.iteritems(), key=itemgetter(1), reverse=True)
    tags = make_tags(sorted_words, minsize = 30, maxsize = 90,colors=random.choice(COLOR_SCHEMES.values()))
    create_tag_image(tags,
                     'tag_image.png',
                     background=(0, 0, 0, 255),
                     size=(750, 300), # 如果设置超过了电脑分辨率就会崩溃
                     layout=LAYOUT_HORIZONTAL,
                     fontname="SimHei")


def test():
    # 适配其他场景只需重写clean_content_cust()方法并重定义cust_stopwords
    sql_statement = "select contents from tweets where id='2174249977' and `No` != 924;"
    make_tag_image(content_jieba(get_content_str(sql_statement), 'TextRank'))