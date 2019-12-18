import flask
import os
from flask import Flask, request, send_from_directory, render_template
import json
import base64
import datetime
import time
import re


class Index:
    def __init__(self, path, target):
        self._index = {}
        self._load(path)
        self._target = target
        print('Loaded {}, with {} indexes, target for {}'.format(path, len(self._index), self._target))

    def __call__(self, key):
        key = str(key)
        with open(os.path.join(self._target, self._get_name(key)), 'r') as f:
            f.seek(self._get_start(key))
            return f.readline().strip().split('\t')[-1]
    
    def _add(self, key, name, start):
        self._index[key] = {
            'name': name,
            'start': int(start)
        }
        
    def _load(self, path):
        filenames = os.listdir(path)
        for name in filenames:
            file = os.path.join(path, name)
            if not os.path.isdir(file) and name[0] != '.':
                with open(file, 'r') as f:
                    for line in f:
                        split = line.strip().split('\t')
                        specs = split[1].split(',')
                        self._add(split[0], specs[0], specs[1])

    def _get_name(self, word):
        return self._index[word]['name']

    def _get_start(self, word):
        return int(self._index[word]['start'])
    

def parse_text(spec):
    spec = spec.strip().split(',')
    start = int(spec[-2])
    length = int(spec[-1])
    text = ''
    if start >= 0 and length >= 0:
        with open(xml_path, 'r') as f:
            f.seek(start)
            text = f.read(length)
    return {
        'title': ''.join(spec[0:-5]),
        'contri_name': spec[-5],
        'contri_id': spec[-4],
        'timestamp': spec[-3],
        'text': text
    }


def parse_list_tf(word):
    spec = tf_index(word)
    spec = spec.strip(';').split(';')[1:]
    for i in range(len(spec)):
        spec[i] = spec[i].split('-')[0:2]
        spec[i][0] = int(spec[i][0])
        spec[i][1] = int(spec[i][1])
        if i > 0 and spec[i][1] == spec[i - 1][1]:
            spec[i][0] += spec[i - 1][0]
    return spec


def tf_retrieval(query, start, amount):
    query = query.strip().lower()
    query = set(re.findall('[A-Za-z]+', query))
    if len(query) == 0:
        return []
    score = {}
    for q in query:
        l = parse_list_tf(q)
        scale_ratio = l[0][1]
        for doc in l:
            if doc[0] not in score:
                score[doc[0]] = 0
            score[doc[0]] += doc[1] / scale_ratio
    score = sorted(score.items(), key=lambda x:x[1], reverse=True)

    result = []
    for doc_id, doc_score in score[start: min(len(score), start + amount)]:
        title = parse_text(text_index(doc_id))['title']
        result.append((title, doc_id, doc_score))
    
    return result


def parse_list_id(word):
    spec = id_index(word)
    spec = spec.strip(';').split(';')[1:]
    for i in range(len(spec)):
        spec[i] = spec[i].split('-')[0:2]
        spec[i][0] = int(spec[i][0])
        spec[i][1] = int(spec[i][1])
        if i > 0:
            spec[i][0] += spec[i - 1][0]
    return spec


def id_retrieval(query, start, amount):
    query = query.strip().lower()
    query = set(re.findall('[A-Za-z]+', query))
    result = []
    if len(query) == 0:
        return []
    word_list = []
    for q in query:
        l = parse_list_id(q)
        word_list.append(l)

    p_list = [0] * len(word_list)
    while p_list[0] < len(word_list[0]):
        doc_id = word_list[0][p_list[0]][0]
        found = True
        for i in range(1, len(word_list)):
            while p_list[i] < len(word_list[i]) and word_list[i][p_list[i]][0] < doc_id:
                p_list[i] += 1
            if p_list[i] >= len(word_list[i]) or word_list[i][p_list[i]][0] != doc_id:
                found *= False
            if found is False:
                break
        if found is True:
            tf_list = []
            for i in range(len(p_list)):
                tf_list.append(word_list[i][p_list[i]][1])
            result.append([doc_id, tf_list])
        p_list[0] += 1
        
    scale_ratio = [0] * len(word_list)
    for i in range(len(result)):
        for j in range(len(result[i][1])):
            scale_ratio[j] = max(scale_ratio[j], result[i][1][j])
            
    final_result = []
    for i in range(len(result)):
        doc_id = result[i][0]
        doc_score = 0
        for score, scale in zip(result[i][1], scale_ratio):
            doc_score += score / scale
        final_result.append((doc_id, doc_score))
    
    final_result.sort(key=lambda k:k[1], reverse=True)
    query_result = []
    for doc_id, doc_score in final_result[start: min(len(final_result), start + amount)]:
        title = parse_text(text_index(doc_id))['title']
        query_result.append((title, doc_id, doc_score))
        
    return query_result
    

def to_list(retrieval):
    l = []
    for i, (title, doc_id, doc_score) in enumerate(retrieval):
        l.append({'number': i, 'title': title, 'id': doc_id, 'score': doc_score, 'url': '/document/{}'.format(doc_id)})
    return l

    
def document_retrieval(doc_id):
    return parse_text(text_index(doc_id))
    

if __name__ == "__main__":
    app = flask.Flask(__name__)
    app.debug = False
    
    # paths
    xml_path = '/mnt/sdb1/chenzhongyu/enwiki-20191101-pages-articles-multistream.xml'
    index_path = '/mnt/sdb1/chenzhongyu/indexes'

    tf_index_path = os.path.join(index_path, 'inverted_index_tf_lossy_index')
    tf_path = os.path.join(index_path, 'inverted_index_tf_lossy')

    id_index_path = os.path.join(index_path, 'inverted_index_id_index')
    id_path = os.path.join(index_path, 'inverted_index_id')

    text_index_path = os.path.join(index_path, 'search_index_index')
    text_path = os.path.join(index_path, 'search_index')
    
    # variables
    tf_index = Index(tf_index_path, tf_path)
    id_index = Index(id_index_path, id_path)
    text_index = Index(text_index_path, text_path)

    @app.route('/', methods=['GET'])
    def index():
        return render_template('index.html')
    
    @app.route('/', methods=['POST'])
    def search():
        start_time = time.time()
        choice = request.form['choice'].strip().upper()
        if choice == 'TF':
            retrieval = tf_retrieval(request.form['query'].lower(), 0, 10)
        else:
            choice = 'ID'
            retrieval = id_retrieval(request.form['query'].lower(), 0, 10)
        stop_time = time.time()
        retrieval = to_list(retrieval)
        return render_template('search.html', data=retrieval, time=stop_time - start_time, query=request.form['query'], choice=choice)
    
    @app.route('/document/<int:doc_id>')
    def document(doc_id):
        start_time = time.time()
        doc = document_retrieval(doc_id)
        end_time = time.time()
        return render_template('document.html', data=doc, time=end_time - start_time, doc_id=doc_id)
        
    app.run(host="10.141.208.18", port=8800)
