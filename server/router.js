filesys = require('fs');
querystring = require('querystring');

orchestrator = require('./orchestrator');

function route(response,pathname,query) {
    console.log("Router receives path " + pathname);
    console.log("Router receives query " + query);
    querymap = querystring.parse(query);
    console.log(querymap);
    if ('p' in querymap && querymap['p'] == 'true') {
            if ('s' in querymap) {
                    if (querymap['s'] == undefined) {    
                        console.error("Router: emr mode: steps file not provided");
                        return 1;
                    }
                    else {
                        console.log("Router: cluster mode");
                        orchestrator.send(response,pathname,null,
                                       null, querymap['s'], true);
                    }
            }
    }
    
    else if ('i' in querymap && querymap['i'] != undefined) {    
        if ('o' in querymap && querymap['o'] != undefined) {    
            console.log("Router: arguments ok");
            console.log("Router: calling Orchestrator");
            orchestrator.send(response,pathname,querymap['i'],
                                  querymap['o'], null, false);
        }
        else {  
            console.log("Router: error");
            response.writeHeader(500, {"Content-Type": "text/plain; charset=utf8"});    
            response.write("Output file not provided\n");    
            response.end();
        }
    } 
    else {  
        console.log("Router: error");
        response.writeHeader(500, {"Content-Type": "text/plain; charset=utf8"});    
        response.write("Input file not provided\n");    
        response.end();
    }
}

exports.route = route;
