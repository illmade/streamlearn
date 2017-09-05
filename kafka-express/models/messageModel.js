//Require Mongoose
var mongoose = require('mongoose');

//Define a schema
var Schema = mongoose.Schema;

var MessageSchema = new Schema({
    topic          : String,
    message        : String
});

//Export function to create "MessageModel" model class
module.exports = mongoose.model('MessageModel', MessageSchema );