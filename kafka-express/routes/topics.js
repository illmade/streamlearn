var express = require('express');
var router = express.Router();

var topic_controller = require('../controllers/topicController');
var message_controller = require('../controllers/messageController');

/* GET topics page. */
router.get('/', function(req, res, next) {
  function cb(topics, error){
	  if (error){
		  return next(error)
	  }
	  console.log("topic list: " + topics);
	  res.render('topicForm', { title: 'Create Messages', topics: topics });
  }
  
  topic_controller.topic_list(cb);
  
});

router.post('/', function(req, res, next) {
  function cb(error, messages){
	  if (error){
		  console.log("we got: " + error);
		  res.render('topicForm', { title: 'Create Messages', errors: error });
	  }
	  console.log("rendering messages");
	  res.render('messages', { title: 'Messages', messages: messages });
  }
  
  message_controller.message_create_post(req, cb);
  
});

module.exports = router;