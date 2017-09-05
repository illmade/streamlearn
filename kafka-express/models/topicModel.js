//Require Mongoose
var mongoose = require('mongoose');

//Define a schema
var Schema = mongoose.Schema;

var TopicSchema = new Schema({
    name          : String
});

//Export function to create "TopicModel" model class
module.exports = mongoose.model('TopicModel', TopicSchema );