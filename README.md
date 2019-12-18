# wikimedia
Wikimedia (enwiki) search engine built on inverted index and tf-idf ranked retrieval
## Data
This information retrieval system is built on [enwiki-20191101-pages-articles-multistream.xml.bz2](https://dumps.wikimedia.org/enwiki/20191101/enwiki-20191101-pages-articles-multistream.xml.bz2)
## Local Environment
* Check out set up procedure in this [blog])https://blog.csdn.net/programmer_wei/article/details/45286749)
* Edit Configurations -> Application -> Program arguments: .jar path + class name + input path + output path
```
$ProjectFileDir$/out/artifacts/Wikipedia/Wikipedia.jar
main.java.Wikipedia
$ProjectFileDir$/input/enwiki_sample.xml
$ProjectFileDir$/output
```

## Author
Zhongyu Chen