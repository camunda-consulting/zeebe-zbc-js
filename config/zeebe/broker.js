'use strict'

const broker = {
  host: '192.168.0.100',
  port: '51015'
}
broker.address = `${broker.host}:${broker.port}`

module.exports = broker
