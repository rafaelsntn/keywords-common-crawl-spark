import argparse
from keybert import KeyBERT
from sentence_transformers import SentenceTransformer
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
from warcio.archiveiterator import ArchiveIterator
import numpy as np
import boto3
from bs4 import BeautifulSoup
from readable_content.parser import ContentParser
import nltk
import re
from urllib.parse import urlparse
import os

def get_article_text(html):
  """
  Get the most relevant keyword sequence for each text using keyBERT.

  :param html: pure html content.
  """
  content_text = ''
  try:
    parser = ContentParser(None, html)
    content = parser.get_content()
    soup = BeautifulSoup(content, 'html.parser')
    content_text = soup.get_text()
  except:
    pass
  return content_text

def get_html_from_warc(warc_path, url_regex_pattern=[]):
  """
  Get the html from warc files.

  :param warc_path: s3 path of the warc segment.
  :param url_regex_pattern: Regex pattern to filter the url.
  """
  s3 = boto3.resource('s3')
  content_list = []
  html_mime_types = ['text/html', 'application/xhtml+xml']
  # download of warc file
  warc_bucket_name, warc_key = warc_path.replace("s3://", "").split("/", 1)
  warc_file_name = warc_key.split('/')[-1]

  try:
    if not os.path.exists(warc_file_name): s3.meta.client.download_file(warc_bucket_name, warc_key, warc_file_name)

    with open(warc_file_name, 'rb') as stream:

      for record in ArchiveIterator(stream):
        if record.rec_type != 'response': continue

        # this is a html content
        content_type = record.rec_headers.get_header('WARC-Identified-Payload-Type')
        if content_type not in html_mime_types: continue

        # url constraints
        url = record.rec_headers.get_header('WARC-Target-URI')
        parsed_url = urlparse(url)

        url_ok_count = 0
        for url_regexp in url_regex_pattern:
          regexp = re.compile(url_regexp)
          if regexp.search(url) is not None: url_ok_count += 1

        if len(url_regex_pattern) > 0 and url_ok_count == 0: continue

        # get html
        html = record.content_stream().read().decode("utf-8", "replace")

        content_list.append((parsed_url.hostname, html))

    os.remove(warc_file_name)
  except Exception as e:
    pass

  return content_list

def get_keyword_seq(hostname, html, ngram_length, bert_transformer, stopwords=[]):
  """
  Get the most relevant keyword sequence for each text using keyBERT.

  :param hostname: the hostname of the content.
  :param html: pure html content.
  :param ngram_length: N of the n-gram.
  :param bert_transformer: The right bert transformer for the language.
  :param stopwords: List of stopwords.
  """
  article_text = get_article_text(html)
  if len(article_text) < 1000: return ('', '') # don't run the inferece on texts with less than 1000 chars

  sentence_model = SentenceTransformer(bert_transformer, cache_folder='.')
  kw_model = KeyBERT(model=sentence_model)
  keywords = kw_model.extract_keywords(article_text, keyphrase_ngram_range=(ngram_length, ngram_length), stop_words=stopwords)
  return (hostname, keywords[0][0]) # hostname and keyword seq

def count_hostnames_for_keyword_seq(warc_list, output_s3_uri, url_regex_pattern, ngram_length, bert_transformer, stopwords):
  """
  Count how many hostnames have each keyword sequence extracted from the texts.

  :param warc_list: List of warc files to read.
  :param output_s3_uri: The URI where output is saved, like an S3 bucket location.
  :param url_regex_pattern: Regex pattern to filter the url.
  :param ngram_length: N of the n-gram.
  :param bert_transformer: The right bert transformer for the language.
  :param stopwords: List of stopwords.
  """
  s3 = boto3.resource('s3')

  with SparkSession.builder.appName("Count hostnames for each keyword sequence").getOrCreate() as spark:

    rdd = spark.sparkContext.parallelize(warc_list)

    count = rdd \
            .flatMap(lambda x: get_html_from_warc(x, url_regex_pattern)) \
            .map(lambda x: get_keyword_seq(x[0], x[1], ngram_length, bert_transformer, stopwords)) \
            .distinct() \
            .map(lambda keyword_seq: (keyword_seq[1], 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .collect()

    np_count = np.array(count)
    bucket_name, prefix = output_s3_uri.replace("s3://", "").split("/", 1)

    # write results
    np.savetxt("output.csv", np_count[np.where(np_count[:,0]!='')], delimiter=",", fmt="%s", encoding='utf8')
    s3.meta.client.upload_file("output.csv", bucket_name, f'{prefix}/output.csv')

if __name__ == "__main__":

  nltk.download('stopwords', download_dir='.')
  nltk.data.path.append('.')

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--warc_list_s3_uri', help="The URI of a text file containing location of warc files.")
  parser.add_argument(
      '--output_s3_uri', help="The URI where output is saved, like an S3 bucket location.")
  parser.add_argument(
      '--url_regex_pattern', help="List of regex pattern to filter the url. Values separated by ;")
  parser.add_argument(
      '--ngram_length', default=3, help="N of the n-gram.")
  parser.add_argument(
      '--bert_transformer', default='sentence-transformers/distiluse-base-multilingual-cased-v1',
      help='The right bert transformer for the language.')
  parser.add_argument(
      '--nltk_stop_word_lang', default='', help='The language of nltk stopwords.')
  args = parser.parse_args()

  # download warc file list
  s3 = boto3.resource('s3')
  bucket_name, key = args.warc_list_s3_uri.replace("s3://", "").split("/", 1)
  s3.meta.client.download_file(bucket_name, key, 'warc_files')
  warc_list = np.loadtxt('warc_files', dtype='str')

  # process warc files
  count_hostnames_for_keyword_seq(warc_list, args.output_s3_uri, args.url_regex_pattern.split(';'), int(args.ngram_length), args.bert_transformer, nltk.corpus.stopwords.words(args.nltk_stop_word_lang))
