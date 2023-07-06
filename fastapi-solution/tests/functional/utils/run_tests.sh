#!/bin/sh

export result="True"

python3 utils/wait_for_es.py > output.txt

resultES=$(cat output.txt)

echo $resultES

if [ $resultES = "True" ]; then
    echo "Elastic True"
    python3 utils/wait_for_redis.py > output2.txt
    resultRedis=$(cat output2.txt)
    if [ $resultRedis = "True" ]; then
        pytest
    else
        echo "False redis"
    fi
else
    echo "False elastic"
fi

exec "$@"