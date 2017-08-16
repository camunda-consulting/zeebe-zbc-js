'use strict'

const config = {
  zeebe: {}
}

config.zeebe.broker = require('./zeebe/broker')

module.exports = config
