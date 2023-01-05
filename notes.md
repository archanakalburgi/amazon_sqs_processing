install localstack
> pip install localstack

start localstack inside a docker container 
>  % localstack start -d

export path
> export PATH=/Users/archanakalburgi/Library/Python/3.8/bin/:$PATH


> export PATH=./Library/Python/3.9/lib/python/site-packages:$PATH



export PATH=/Users/archanakalburgi/Library/Python/3.9/bin/:$PATH

export PATH=/Users/archanakalburgi/Library/Python/3.9/lib/python/site-packages/awscli/:$PATH



awslocal sqs create-queue --queue-name sample-queue '{ "QueueUrl": "http://localhost:4566/000000000000/sample-queue" }'

awslocal sqs create-queue --queue-name sample-queue
{
    QueueUrl : "http://localhost:4566/000000000000/sample-queue"
}


fetch python3 -m venv fetch-env
source fetch-env/bin/activate

git clone https://bitbucket.org/fetchrewards/data-engineering-take-home.git


# Murmur3 hash
python -m pip install psycopg2-binary
python -m pip install pandas 
python -m pip install murmurhash3

##
mmh3.hash('foo')

## Run
python /Users/archanakalburgi/fetch/data-engineering-take-home/process_sqs.py