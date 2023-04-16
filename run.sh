source ~/.bashrc
pkill python
workspace=/mnt/d/workspace/qtrade
cd ${workspace}
git pull
interpreter=/home/zxl/anaconda3/bin/python
alias python3=${interpreter}
export PYSPARK_PYTHON=${interpreter}
export PYSPARK_DRIVER_PYTHON=${interpreter}
${interpreter} qtrade.py >nohup.out 2>&1
git add *
git commit -m "auto commit"
git push origin master
