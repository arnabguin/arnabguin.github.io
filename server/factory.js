
child_process = require('child_process');

function createOperation(response,klass,op,i,o,s,p) {
    console.log("Factory: class=%s,op=%s,i=%s,o=%s,s=%s,p=%d", klass, op, i, o, s, p);
    emr = p;
    switch (op) {
        case 'add':
             switch (klass) {
                 case 'matrix':
                     console.time('Time');
                     console.log("Factory:Matrix:Add: i=%s,o=%s",i,o);
                     if (emr) {
                         mplusm = child_process.spawn(process.env.MATHIOJS_HOME + '/scripts/runJob.py', ['--emr', emr, '-s', s]); 
                     }
                     else {
                         mplusm = child_process.spawn(process.env.MATHIOJS_HOME + '/scripts/runJob.py', ['-j', process.env.MATHIOJS_HOME + '/target/matrix.add-v1.0.jar', '-p', 'org.mathio.js.MatrixAdd', '-i', i, '-o', o]); 
                     }
                     console.log("Factory:Matrix:Add: starting operation, steps=%s,p=%d",s,p);
                     child = mplusm;
                     break;
                 default:
                     console.error("Class not supported :" + klass);
                     return 1;
             }
             break;
        case 'multiply':
             switch (klass) {
                 case 'matrix':
                     console.time('Time');
                     console.log("Factory:Matrix:Multiply: i=%s,o=%s",i,o);
                     if (emr) {
                         mtimesm = child_process.spawn(process.env.MATHIOJS_HOME + '/scripts/runJob.py', ['--emr', emr, '-s', s]); 
                     }
                     else {
                         mtimesm = child_process.spawn(process.env.MATHIOJS_HOME + '/scripts/runJob.py', ['-j', process.env.MATHIOJS_HOME + '/target/matrix.multiply-v1.0.jar', '-p', 'org.mathio.js.MatrixMultiply', '-i', i, '-o', o]); 
                     }
                     console.log("Factory:Matrix:Multiply: starting operation, steps=%s,p=%d",s,p);
                     child = mtimesm;
                     break;
                 default:
                     console.error("Class not supported :" + klass);
                     return 1;
             }
             break;
       default:
           console.error("Operation not supported :" + op); 
           return 1;
    }
    child.stderr.on('data', function (data) {
        if (/Error/.test(data)) {
            response.write(data);
        }
        console.log('stderr: ' + data);
        console.timeEnd('Time');
    });
    child.stdout.on('data', function (data) {
        if (emr) {
            response.write(data);
        }
        console.log('stdout: ' + data);
        console.timeEnd('Time');
    });
    child.stdin.on('data', function (data) {
        console.log("Child received instructions :" + data);
        console.time('Time');
    });
    child.on('close', function(code) {
        console.log("Child exited with status " + code); 
        if (code) {
            response.write("Operation failed."); 
            response.end();
        }
        else {
            response.write("Operation succeeded."); 
            response.end();
        }
    });
};

exports.createOperation = createOperation
