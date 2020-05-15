# spark-wikipedia-edit-stream

This project will get you started in space of spark
streaming. 

As an example English Wikipedia edit stream is used
with spark streaming. 

### Requirements
The code is tested with `Python3.7`. Install following
packages using `pip` preferably in a virtual 
environment.
    
    1. pywikibot
    2. findspark
    3. matplotlib
    4. jupyter notebook


### Run
Start jupyter notebook with `pyspark`. The code is
tested with `Spark 2.4.5` running on `YARN`. To install
`Spark` and `Hadoop` follow [this](https://github.com/InzamamAnwar/raspberrypi-hadoop-cluster)
guide.

Run [stream.py](./stream.py) to setup server over which
English Wikipedia edit stream is written. Run the following
command with example parameters. 
```shell script
python stream.py "en.wikipedia.org" 5050 "20200515"
```
1. **en.wikipedia.org**:   English wikipedia edit stream is used here for example. Other languages can be used,
 like for `German` use `de.wikipedia.org`.

2. **5050**:   is the port at which edit stream will be written.

3. **"20200515"**:   date from where edits will be reported till the latest. Format of date is `YYYYMMDD`.


Finally, open [spark_stream](./spark_stream.ipynb) and run the cells 
step by step.