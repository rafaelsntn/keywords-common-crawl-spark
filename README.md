# Keywords extraction from Common Crawl using Apache Spark 

This project provides a script that extracts keywords that represents a web page and aggregates them by counting the occurence of each one.

## Running locally
### Install the project and packages:
1. Clone the GitHub repo <br />
`git clone https://github.com/rafaelsntn/deepspeech-batch-inference-aws`
2. Create a virtual environment: <br />
`python -m venv .venv`
3. Activate the virtual environment: <br />
In Linux: <br />
`source .venv/bin/activate` <br />
In Windows: <br />
`.venv\Scripts\activate.bat`
4. Once the virtualenv is activated, you can install the required dependencies: <br />
`pip install -r requirements.txt`

### Prepare AWS S3:
1. Create a bucket on AWS S3.
2. Upload a file warc_files.txt to AWS S3 bucket, with the Common Crawl warc segments. This repo provides a file with two segments as an example.

### Run:
A example that runs on webpages with url patterns "blog.com.br" and ".com.br/blog" and uses portuguese stopwords: <br />
`python keywords_cc.py --warc_list_s3_uri s3://BUCKET-NAME/warc_files.txt --output_s3_uri s3://BUCKET-NAME/output --ngram_length 2 --url_regex_pattern "\.com\.br/blog,://blog\..*\.com\.br,\.blog\..*\.com\.br" --nltk_stop_word_lang portuguese`

## Running on AWS EMR

## License

This solution is licensed under the MIT License. See the LICENSE file.