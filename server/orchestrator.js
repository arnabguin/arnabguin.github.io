
filesys = require('fs');
url = require('url');

dirsys = require('./dirsys');
factory = require('./factory');

function send(response, path, inputfile, outputfile, steps, cloud) {
    pathcomponents = path.split("/");
    nil = pathcomponents[0];
    klass = pathcomponents[1];
    op = pathcomponents[2]; 

    if (inputfile && outputfile) {

    uri = url.parse(inputfile);
    uro = url.parse(outputfile);
    console.log("Input URL protocol : %s",uri.protocol);
    console.log("Output URL protocol : %s",uro.protocol);

    if (uri.protocol == "file://") {
        if (!dirsys.existsSync(uri.pathname)) {
            console.error("Orchestrator: input directory does not exist: %s. MATHIOJS_HOME may not be set on client.", inputfile);
            response.writeHeader(500, {"Content-Type": "text/plain; charset=utf8"});
            response.write("Input file does not exist: " + inputfile + ".Please check if MATHIOJS_HOME is set");
            return 1;
        }
    }
    else if (uri.protocol == null) {
        if (!dirsys.existsSync(uri.pathname)) {
            console.error("Orchestrator: input directory does not exist: %s. MATHIOJS_HOME may not be set on client.", inputfile);
            response.writeHeader(500, {"Content-Type": "text/plain; charset=utf8"});
            response.write("Input file does not exist: MATHIOJS_HOME may not be set." +  inputfile);
            return 1;
        }
    }
    if (uro.protocol == "file://") {
        if (dirsys.existsSync(uro.pathname)) {
            console.warn("Orchestrator: output directory exists: %s", outputfile);
            response.writeHeader(200, {"Content-Type": "text/plain; charset=utf8"});
            response.write("Output directory exists: " + outputfile);
        }
    }
    else if (uro.protocol == null) {
        if (dirsys.existsSync(uro.pathname)) {
            console.error("Orchestrator: output directory exists: %s", outputfile);
            response.writeHeader(200, {"Content-Type": "text/plain; charset=utf8"});
            response.write("Output directory exists: " + outputfile);
        }
    }
    }
    if (cloud) {  // cloud mode will override user settings
        if (steps == null) {
            console.error("Orchestrator: no steps file provided: %s", steps);
            response.writeHeader(500, {"Content-Type": "text/plain; charset=utf8"});
            response.write("Steps file does not exist: " + steps + ".Kindly check if environment variable MATHIOJS_HOME is set");
            return 1;
        }
            
        urs = url.parse(steps);
        console.log("Steps URL protocol : %s",urs.protocol);
        if (urs.protocol == "file://") {
            if (!filesys.existsSync(urs.pathname)) {
                console.error("Orchestrator: steps file does not exist: %s", steps);
                response.writeHeader(500, {"Content-Type": "text/plain; charset=utf8"});
                if (process.env.MATHIOJS_HOME == "") {
                    response.write("MATHIOJS_HOME not set");
                }
                else {
                    response.write("Steps file does not exist: " + steps);
                }
                return 1;
            }
        }
        else if (urs.protocol == null) {
            if (!filesys.existsSync(urs.pathname)) {
                console.error("Orchestrator: steps file does not exist: %s", steps);
                response.writeHeader(500, {"Content-Type": "text/plain; charset=utf8"});
                response.write("Steps file does not exist: " +  steps + ". MATHIOJS_HOME may not be set.");
                return 1;
            }
        }
    }
    
    console.log("Orchestrator: path=%s,i=%s,o=%s,s=%s,p=%d", path, inputfile, outputfile,steps,cloud);
    switch (klass) {
        case 'matrix':
            console.log("Class=" + 'matrix');
            break;
        case 'vector':
            console.log("Class=" + 'vector');
            break;
        default:
            console.error("Error: There is no class by the name of " + klass);
            return 1;
    }
    console.log("Orchestrator: dispatching create request to Factory");
    factory.createOperation(response, klass, op, inputfile, outputfile,steps,cloud);
}

exports.send = send 
