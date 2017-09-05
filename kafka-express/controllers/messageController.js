var Message = require('../models/keyedMessageModel');

var kafka = require('kafka-node'),
	Producer = kafka.Producer,
	Consumer = kafka.Consumer,
	KeyedMessage = kafka.KeyedMessage,
	client = new kafka.Client();

producer = new Producer(client);

producer.on('ready', function () {
	console.log("producer ready");
});

producer.on('error', function (kafka_err) {
		console.log("producer error " + kafka_err);
	}
)

var consumer = new Consumer(
    client,
    [
        { topic: '595a62a1641d21dedbfcb364', partition: 0 }, 
        { topic: '595a62a1641d21dedbfcb365', partition: 0 }
    ],
    {
        autoCommit: false
    }
);

consumer.on('message', function (kafkaMessage) {
	console.log("client got message for topic: " + JSON.stringify(kafkaMessage));
});

consumer.on('error', function (kafkaMessage) {
	console.log("client got error: " + JSON.stringify(kafkaMessage));
});

function censor(censor) {
  var i = 0;

  return function(key, value) {
    if(i !== 0 && typeof(censor) === 'object' && typeof(value) == 'object' && censor == value) 
      return '[Circular]'; 

    if(i >= 29) // seems to be a harded maximum of 30 serialized objects?
      return '[Unknown]';

    ++i; // so we know we aren't using the original object anymore

    return value;  
  }
}

//Display list of all messages
exports.message_list = function(topic, cb) {
	
	if (topic) {
		Message.find({'topic': topic}).exec(function (err, messages) {
	        if (err) { 
	        	cb(err);
	        }
	        cb(null, messages);
	      });
	}
	else {
		Message.find().exec(function (err, messages) {
	        if (err) { 
	        	cb(err);
	        }
	        cb(null, messages);
	      });
	}
   
};

//Handle Topic create on POST 
exports.message_create_post = function(req, cb) {
    //Check that the topic field is not empty
    req.checkBody('topic', 'Topic required').notEmpty(); 
    req.checkBody('key', 'Message key required').notEmpty(); 
    req.checkBody('message', 'Message name required').notEmpty(); 
    
    //Trim and escape the fields. 
    req.sanitize('topic').escape();
    req.sanitize('topic').trim();

    req.sanitize('key').escape();
    req.sanitize('key').trim();
    
    req.sanitize('message').trim();
    
    //Run the validators
    var errors = req.validationErrors();

    if (errors) {
    	console.log("validation errors" +  errors);
        cb(errors);
    return;
    } 
    else {
        //Create a message object with escaped and trimmed data.
    	
    	var body = JSON.parse(req.body.message);
    	
    	var time = new Date().toISOString();
    	var jsonBody = {"action": body, "time": time };

    	var mongooseBody = { topic: req.body.topic, key: req.body.key, message: JSON.stringify(body) };
    	var messageBody =  { topic: req.body.topic, key: req.body.key, message: JSON.stringify(jsonBody) };
    	//kafka-node likes messages to be sent together so the format is different than for mongoose
    	
    	var keyedMessage = new KeyedMessage(req.body.key, messageBody.message);
    	
    	var payloads = [{ topic: messageBody.topic, key: req.body.key, messages: [keyedMessage] }];
        
	    producer.send(payloads, function (err, data) {
	    	console.log("producer got data: " + JSON.stringify(data) + " and errors: " + 
	    			JSON.stringify(err, censor(err)));
	    	if(err){
	    		cb(err);
	    	}
	    	else {
	    		//create the message for mongoose
	    		var message = new Message(mongooseBody);
	    		message.save(function (err) {
		        	if (err) {
		        	   console.log("message save error: " + err);
		          	   return cb(err); 
		            }
		        	exports.message_list(messageBody.topic, function(list_err, messages){
		        		if (list_err) {
		        			//if list fails for some reason it isn't fatal - just return the added message
		        			console.log("list error: " + list_err);
		        			cb(null, [messageBody]);
		                }
		        		console.log("handling message list");
		                cb(null, messages);
		        	});
		        });  
	    	}
	    });
    }

};
