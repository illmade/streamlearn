var frisby = require('frisby');
var fs = require('fs');
var parse = require('csv-parse');
var transform = require('stream-transform');
var _ = require('lodash');
var async = require('async');

var now = new Date().toISOString();
frisby.create('Ensure response has proper JSON types in specified keys')
  .post('http://localhost:3000/topics', {
    topic: "595a62a1641d21dedbfcb364",
    message: '{"action": {"name": "testName", "hours": 16}, "time": "' + now + '"}',
    key: "test"
  })
  .expectStatus(200)
  .expectHeader("Content-Type",	"text/html; charset=utf-8")
.toss()