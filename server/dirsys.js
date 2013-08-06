
fs = require('fs');

function existsSync (d) {
  try { return fs.statSync(d).isDirectory() }
  catch (er) { return false }
}

exports.existsSync = existsSync;
