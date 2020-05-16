# Data etl

This is where data is extracted from the offical API for the Fantasy Premier League, performs basic transformations, and 

### Note

Note that a lot of this is currently old scripty code which I plan to tidy up; I just want to get the basic framework up and going. The code does the job but is far from elegant!

There is the option of using more sophisticated tooling which I may look into eventually. However, as the data is small and infrequently updated, there's little need to do this right now.

## Programs
- **run_extract.py:** Pulls the data from the API and saves locally. Note, a version which saves directly to S3 as a AWS Lambda function can be found in aws_lambda/. That is the version in use. Makes use of functions inside of **extract.py**.
- **run_transform.py:** Simple transformations including extracting data sets from API response and cleaning. Saves locally. Makes use of functions in **transform.py**.
- **run_load.py:** Takes saved data sets and loads into a postgres database.
- **etl_full_wrapper.bash:** Simple bash script to act as pipeline for above Python programs.
- **run_data_transform_aws.py:** Script to run transforms on an EC2 instance. Deployed with the **Dockerfile**.
