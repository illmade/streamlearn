var Topic = require('../models/topicModel');

//Display list of all Topics
exports.topic_list = function(cb) {
	
	Topic.find()
    .exec(function (err, topics) {
        if (err) { 
        	cb(null, err);
        }
        cb(topics);
      });
	
};

exports.topic_create_get = function(req, res, next) {       
    res.render('topic_form', { title: 'Create Topic' });
};

//Handle Topic create on POST 
exports.topic_create_post = function(req, res, next) {
    
    //Check that the name field is not empty
    req.checkBody('name', 'Topic name required').notEmpty(); 
    
    //Trim and escape the name field. 
    req.sanitize('name').escape();
    req.sanitize('name').trim();
    
    //Run the validators
    var errors = req.validationErrors();

    if (errors) {
        //If there are errors render the form again, passing the previously entered values and errors
        res.render('topic_form', { title: 'Topic Genre', topic: topic, errors: errors});
    return;
    } 
    else {
    	//Create a genre object with escaped and trimmed data.
        var topic = new Topic(
          { name: req.body.name }
        );
        
        // Check if Topic with same name already exists
        Topic.findOne({ 'name': req.body.name })
            .exec( function(err, found_genre) {
                 console.log('found_topic: ' + found_topic);
                 if (err) { return next(err); }
                 
                 if (found_topic) { 
                     //Genre exists, redirect to its detail page
                     res.redirect(found_topic.url);
                 }
                 else {
                     topic.save(function (err) {
                       if (err) { return next(err); }
                       //Genre saved. Redirect to genre detail page
                       res.redirect(genre.url);
                     });
                 }
                 
             });
    }

};