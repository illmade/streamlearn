//Require Mongoose
var mongoose = require('mongoose');

//Define a schema
var Schema = mongoose.Schema;

var KeyedMessageSchema = new Schema({
    topic          : String,
    key            : String,
    message        : String
});

//Export function to create "MessageModel" model class
module.exports = mongoose.model('KeyedMessageModel', KeyedMessageSchema);