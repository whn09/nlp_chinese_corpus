import pandas as pd
import json
import jieba
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: f'{x:.3f}')


def jieba_cut(sentence):
    words = jieba.cut(sentence)
    words = ' '.join(words)
    return words


def baike_qa2019(filename):
    i = 0
    qid = []
    category = []
    title = []
    desc = []
    answer = []
    with open(filename, 'r') as fin:
        line = fin.readline()
        while line is not None and line != '':
            data = json.loads(line)
            # print(i, data)
            qid.append(data['qid'])
            category.append(data['category'])
            title.append(data['title'])
            desc.append(data['desc'])
            answer.append(data['answer'])
            line = fin.readline()
            i += 1
    data = pd.DataFrame({'qid': qid, 'category': category, 'title': title, 'desc': desc, 'answer': answer})
    print(data.head())
    data.to_csv(filename+'.csv')
    data['category'] = '__label__'+data['category']
    data['title_cut'] = data['title'].apply(lambda x: jieba_cut(x))
    data['desc_cut'] = data['desc'].apply(lambda x: jieba_cut(x))
    data['answer_cut'] = data['answer'].apply(lambda x: jieba_cut(x))
    data.to_csv(filename+'.title'+'.tsv', columns=['category', 'title_cut'], sep='\t', index=False, header=False)
    data.to_csv(filename+'.desc'+'.tsv', columns=['category', 'desc_cut'], sep='\t', index=False, header=False)
    data.to_csv(filename+'.answer'+'.tsv', columns=['category', 'answer_cut'], sep='\t', index=False, header=False)
    return


def translation2019zh(filename):
    i = 0
    english = []
    chinese = []
    with open(filename, 'r') as fin:
        line = fin.readline()
        while line is not None and line != '':
            data = json.loads(line)
            # print(i, data)
            english.append(data['english'])
            chinese.append(data['chinese'])
            line = fin.readline()
            i += 1
    data = pd.DataFrame({'english': english, 'chinese': chinese})
    print(data.head())
    data.to_csv(filename+'.csv')
    data['chinese_cut'] = data['chinese'].apply(lambda x: jieba_cut(x))
    data.to_csv(filename+'.chinese'+'.tsv', columns=['chinese_cut'], sep='\t', index=False, header=False)
    return


def webtext2019zh(filename):
    i = 0
    qid = []
    title = []
    desc = []
    topic = []
    star = []
    content = []
    answer_id = []
    answerer_tags = []
    with open(filename, 'r') as fin:
        line = fin.readline()
        while line is not None and line != '':
            data = json.loads(line)
            # print(i, data)
            qid.append(data['qid'])
            title.append(data['title'])
            desc.append(data['desc'])
            topic.append(data['topic'])
            star.append(data['star'])
            content.append(data['content'])
            answer_id.append(data['answer_id'])
            answerer_tags.append(data['answerer_tags'])
            line = fin.readline()
            i += 1
    data = pd.DataFrame({'qid': qid, 'title': title, 'desc': desc, 'topic': topic, 'star': star, 'content': content, 'answer_id': answer_id, 'answerer_tags': answerer_tags})
    print(data.head())
    data.to_csv(filename+'.csv')
    data['topic'] = '__label__'+data['topic']
    data['title_cut'] = data['title'].apply(lambda x: jieba_cut(x))
    data['desc_cut'] = data['desc'].apply(lambda x: jieba_cut(x))
    data['content_cut'] = data['content'].apply(lambda x: jieba_cut(x))
    data.to_csv(filename+'.title'+'.tsv', columns=['topic', 'title_cut'], sep='\t', index=False, header=False)
    data.to_csv(filename+'.desc'+'.tsv', columns=['topic', 'desc_cut'], sep='\t', index=False, header=False)
    data.to_csv(filename+'.content'+'.tsv', columns=['topic', 'content_cut'], sep='\t', index=False, header=False)
    return


def wiki_zh(dirname):
    i = 0
    id = []
    url = []
    title = []
    text = []
    subdirs = os.listdir(dirname)
    for subdir in subdirs:
        subdir = os.path.join(dirname, subdir)
        if os.path.isdir(subdir):
            filenames = os.listdir(subdir)
            for filename in filenames:
                filename = os.path.join(subdir, filename)
                with open(filename, 'r') as fin:
                    line = fin.readline()
                    while line is not None and line != '':
                        data = json.loads(line)
                        # print(i, data)
                        id.append(data['id'])
                        url.append(data['url'])
                        title.append(data['title'])
                        text.append(data['text'].replace('\n', '\\n'))
                        line = fin.readline()
                        i += 1
    data = pd.DataFrame({'id': id, 'url': url, 'title': title, 'text': text})
    print(data.head())
    data.to_csv(dirname+'/'+dirname+'.csv')
    data['title_cut'] = data['title'].apply(lambda x: jieba_cut(x))
    data['text_cut'] = data['text'].apply(lambda x: jieba_cut(x))
    data.to_csv(dirname+'/'+dirname+'.title'+'.tsv', columns=['title_cut'], sep='\t', index=False, header=False)
    data.to_csv(dirname+'/'+dirname+'.text'+'.tsv', columns=['text_cut'], sep='\t', index=False, header=False)
    return


def new2016zh(filename):
    i = 0
    news_id = []
    keywords = []
    desc= []
    title = []
    source = []
    time = []
    content = []
    with open(filename, 'r') as fin:
        line = fin.readline()
        while line is not None and line != '':
            data = json.loads(line)
            # print(i, data)
            news_id.append(data['news_id'])
            keywords.append(data['keywords'])
            desc.append(data['desc'])
            title.append(data['title'])
            source.append(data['source'])
            time.append(data['time'])
            content.append(data['content'])
            line = fin.readline()
            i += 1
    data = pd.DataFrame({'news_id': news_id, 'keywords': keywords, 'desc': desc, 'title': title, 'source': source, 'time': time, 'content': content})
    print(data.head())
    data.to_csv(filename+'.csv')
    data['desc_cut'] = data['desc'].apply(lambda x: jieba_cut(x))
    data['title_cut'] = data['title'].apply(lambda x: jieba_cut(x))
    data['content_cut'] = data['content'].apply(lambda x: jieba_cut(x))
    data.to_csv(filename+'.desc'+'.tsv', columns=['desc_cut'], sep='\t', index=False, header=False)
    data.to_csv(filename+'.title'+'.tsv', columns=['title_cut'], sep='\t', index=False, header=False)
    data.to_csv(filename+'.content'+'.tsv', columns=['content_cut'], sep='\t', index=False, header=False)
    return


if __name__ == '__main__':
    # baike_qa2019('chinese-nlp-corpus/baike_qa2019/baike_qa_train.json')
    # baike_qa2019('chinese-nlp-corpus/baike_qa2019/baike_qa_valid.json')
    # translation2019zh('chinese-nlp-corpus/translation2019zh/translation2019zh_train.json')
    # translation2019zh('chinese-nlp-corpus/translation2019zh/translation2019zh_valid.json')
    # webtext2019zh('chinese-nlp-corpus/webtext2019zh/web_text_zh_train.json')
    # webtext2019zh('chinese-nlp-corpus/webtext2019zh/web_text_zh_testa.json')
    # webtext2019zh('chinese-nlp-corpus/webtext2019zh/web_text_zh_valid.json')
    # wiki_zh('chinese-nlp-corpus/wiki_zh')
    # new2016zh('chinese-nlp-corpus/new2016zh/news2016zh_train.json')
    # new2016zh('chinese-nlp-corpus/new2016zh/news2016zh_valid.json')
