'use strict'

const Struct = require('ref-struct')

// define object GoString to map:
// C type struct { const char *p; GoInt n; }
const GoString = Struct({
  p: 'string',
  n: 'longlong'
})

const CreateGoString = function (value) {
  const newString = new GoString()
  newString.p = value
  newString.n = eval(value.length)
  return newString
}

const go = {
  GoString, CreateGoString
}


module.exports = go
