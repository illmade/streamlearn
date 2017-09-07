# streamlearn

nodejs to kafka to spark-ml

I was interested in using Kafka as the head of a continuous application environment: 
all events writing to kafka first and everything else reacting. This was something I
had seen as an idea but without much in the way of implementation.

So here we create a minimal nodejs-express app. To connect to kafka we use

var kafka = require('kafka-node'),

So all events are pushed to kafka and when it reports back we use mongoose to connect to a mlab.com mongodb instance
for storage or react in anyway we want.

# generate traffic

lets push some traffic to the nodejs app: we use some [census data](https://archive.ics.uci.edu/ml/datasets/Census+Income)

this is sent to the nodejs app which adds it to a kafka stream.

listening to the kafka stream is a simple spark streaming app which feeds each entry through
a Gradient Boosted Tree model - trained using SplitCensus and ProcessCensus in
[learn](learn/src/main/scala/com/xythings/learn/)

# but why?

with a tiny bit of kafka code on the nodejs app events can be stored and maintained. Kafka can deal 
with the lifecycle by applying topic based rules.

Monitoring doesn't have to go to a database and it can run high cost evaluations like machine learning at will
from other machines.


