'use strict'

const ffi = require('ffi')
const config = require('../config')
const { GoString, CreateGoString } = require('../models/go')

const client = {}
client.goClient = {}

client.init = function () {
  // define foreign functions
  client.goClient = ffi.Library('./go/zeebe-go', {
    subscribe: ['string', [GoString, GoString, 'int', GoString, GoString]],
    deployWorkflow: ['string', [GoString, GoString, GoString]],
    startWorkFlowInstance: ['string', [GoString, GoString, GoString]]
  })
}

client.subscribe = function () {
  const broker = new CreateGoString(config.zeebe.broker.address)
  const topic = new CreateGoString(config.zeebe.broker.defaulttopic)
  const zbc = new CreateGoString('zbc')
  const foo = new CreateGoString('foo')
  client.goClient.subscribe(broker, topic, 0, zbc, foo)
}
client.deployWorkflow = function () {
  const workflowPath = new CreateGoString('./examples/demoProcess.bpmn')
  const broker = new CreateGoString(config.zeebe.broker.address)
  const topic = new CreateGoString(config.zeebe.broker.defaulttopic)
  client.goClient.deployWorkflow(workflowPath, broker, topic)
}
client.startWorkflowInstance = function () {
  const cmdPath = new CreateGoString('./examples/create-workflow-instance.yaml')
  const broker = new CreateGoString(config.zeebe.broker.address)
  const topic = new CreateGoString(config.zeebe.broker.defaulttopic)

  client.goClient.startWorkFlowInstance(cmdPath, broker, topic)
}


module.exports = client
