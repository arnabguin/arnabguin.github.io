
sys = require("sys"),  
http = require("http"),  
path = require("path"),  
url = require("url"),  
filesys = require("fs");  

router = require("./router");

function start(port) {

  server = http.createServer(function(request,response) {  
    var klassop = url.parse(request.url).pathname;  
    var query = url.parse(request.url).query;
    console.log("Server: received path " + klassop);
    console.log("Server: received query " + query);
    console.log("Server: validating class and operation");
    console.log(klassop);
    if (process.env.MATHIOJS_HOME == null) {
        response.writeHeader(500, {"Content-Type": "text/plain; charset=utf8"});
        response.write("MATHIOJS_HOME environment variable not set\n");
        response.end();
    }
    else {
        filesys.exists(process.env.MATHIOJS_HOME, function(exists) { 
            if (!exists) {
                response.writeHeader(500, {"Content-Type": "text/plain; charset=utf8"});
                response.write("MATHIOJS_HOME environment variable not set\n");
                response.end();    
            }
        });
    }
    console.log("Server: feature exists?: " + process.env.MATHIOJS_HOME + '/server' + klassop);
    filesys.exists(process.env.MATHIOJS_HOME + '/server' + klassop,function(exists) {  
        if (!exists){  
            response.writeHeader(404, {"Content-Type": "text/plain; charset=utf8"});    
            response.write("Operation not found.Also please check if MATHIOJS_HOME environment variable is set\n");    
            response.end();  
        }  
        else {  
            if (klassop == "/index.html" || klassop == "") {
                filesys.readFile('./index.html', 'utf8', function (err, data) {
                    if (err) { 
                        console.log("Server: Critical: Unable to load main page ");
                        response.writeHeader(404, {"Content-Type": "text/plain; charset=utf8"});
                        response.write("Server maintenance in progress. Please check back later");
                    } else {
                        response.writeHeader(200, {"Content-Type": "text/html; charset=utf8"});
                        response.write(data);
                    }
                    response.end();
                }); 
            }
            else if (klassop == "/d3.html") {
                filesys.readFile('./d3.html', 'utf8', function (err, data) {
                    if (err) { 
                        console.log("Server: Critical: Unable to load main page ");
                        response.writeHeader(404, {"Content-Type": "text/plain; charset=utf8"});
                        response.write("Server maintenance in progress. Please check back later");
                    } else {
                        response.writeHeader(200, {"Content-Type": "text/html; charset=utf8"});
                        response.write(data);
                    }
                    response.end();
                }); 
            }
            else if (klassop == "/images/Chaos.png" || klassop == "/css/base" || klassop == "/css/stylesheet") {
            }
            else {
                console.log("Server: validated class and operation ");
                console.log("Server: routing request to Router");
                router.route(response,klassop,query);
            }
        }  
    });  
  });
  server.listen(port);
  sys.puts("Server Running on " + port);   

}


exports.start = start
