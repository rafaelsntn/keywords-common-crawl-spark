# Keywords extraction from Common Crawl using Apache Spark 

This project provides a script that extracts keywords that represents a web page and aggregates them by counting the occurence of each one.

## Running locally
### Install the project and packages:
1. Clone the GitHub repo <br />
`git clone https://github.com/rafaelsntn/keywords-common-crawl-spark`
`cd keywords-common-crawl-spark`
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
2. Upload a file warc_files.txt to the AWS S3 bucket, containing the Common Crawl warc segments. This repo provides a file with two segments as an example.

### Run:
A example that runs on webpages with url patterns "blog.com.br" and ".com.br/blog" and uses portuguese stopwords: <br />
`python keywords_cc.py --warc_list_s3_uri s3://YOUR-BUCKET-NAME/warc_files.txt --output_s3_uri s3://YOUR-BUCKET-NAME/output --ngram_length 2 --url_regex_pattern "\.com\.br/blog,://blog\..*\.com\.br,\.blog\..*\.com\.br" --nltk_stop_word_lang portuguese`

## Running on AWS EMR
1. First follow steps in the "Running locally" section.
2. Upload the files emr_spark_config.json and emr_python_packages.sh to the AWS S3 bucket created in the "Running locally" section.
3. Create a cluster in EMR using AWS CLI. Example: <br />
`
aws emr create-cluster \
--name "Spark cluster" \
--release-label emr-5.34.0 \
--applications Name=Spark \
--instance-type m5.xlarge \
--instance-count 3 \
--bootstrap-actions Path="s3://YOUR-BUCKET-NAME/python_packages.sh" \
--use-default-roles \
--log-uri s3://YOUR-BUCKET-NAME/logs/ \
--enable-debugging \
--configurations file://./spark_config.json
`
<br />
Replace "\" with "^" for Windows. <br />
4. After cluster changes to Running status, create a task in EMR passing the cluster id. Example: <br />
`
aws emr add-steps \
--cluster-id <your cluster id> \
--steps Type=Spark,Name="Keywords CC",ActionOnFailure=CONTINUE,Args=[s3://YOUR-BUCKET-NAME/keywords_cc.py,--warc_list_s3_uri,s3://YOUR-BUCKET-NAME/warc_files,--output_s3_uri,s3://YOUR-BUCKET-NAME/output_2_gram,--ngram_length,2,--url_regex_pattern,\.com\.br/blog;://blog\..*\.com\.br;\.blog\..*\.com\.br",--nltk_stop_word_lang,portuguese]
`

## Cleaning up
After running the tasks, you can terminate the cluster in the AWS console or via CLI, using the following command: <br />
`aws emr terminate-clusters --cluster-ids <your cluster id>`

## License

This solution is licensed under the MIT License. See the LICENSE file.