var express = require('express');
var router = express.Router();
var _ = require('lodash');

var message_controller = require('../controllers/messageController');

/* GET messages page. */
router.get('/', function(req, res, next) {

  function cb(err, messages){
	  
	  if (err){
		  return next(err)
	  }
	  
	  var ordered = _.orderBy(messages, ['topic']);
	  
	  res.render('messages', { title: 'Messages', messages: ordered});
  }
  
  message_controller.message_list(null, cb);
  
});

module.exports = router;