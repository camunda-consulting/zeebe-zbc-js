'use strict'

const broker = {
  host: '192.168.99.100',
  port: '51015',
  defaulttopic: 'default-topic'
}
broker.address = `${broker.host}:${broker.port}`

module.exports = broker
