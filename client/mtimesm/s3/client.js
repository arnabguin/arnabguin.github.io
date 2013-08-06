var http=require('http');

//make the request object
var options = {
  'host': 'localhost',
  'port': 1135,
  'path': '/matrix/multiply?i=s3n://mathiojs/data&&o=s3n://mathiojs/data_out&&s=' + process.env.MATHIOJS_HOME + '/client/mtimesm/s3/steps.json&&p=true',
  'method': 'GET'
};

var req = http.request(options, function(response) {
  console.log('STATUS: ' + response.statusCode);
  console.log('HEADERS: ' + JSON.stringify(response.headers));
  response.setEncoding('utf8');
  response.on('data', function (chunk) {
    console.log(chunk);
  });
});

req.on('error', function(e) {
  console.log('problem with request: ' + e.message);
});

req.end();

