'use strict'

const client = require('./client')

const myConsole = console

if (process.argv.length <= 2) {
  myConsole.log(`Usage: ${__filename} SOME_PARAM`)
  process.exit(-1)
}

const param = process.argv[2]

myConsole.log(`Param: ${param}`)

client.init()
client[param]()
