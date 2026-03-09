import 'reflect-metadata'
import {
  ServiceBusClient,
  ServiceBusAdministrationClient,
  ServiceBusMessage
} from '@azure/service-bus'
import { Message, MessageAttributes } from '@node-ts/bus-messages'
import { TestSystemMessage, transportTests } from '@node-ts/bus-test'
import {
  AzureServiceBusTransport,
  fromApplicationProperties,
  normalizeTopicName
} from './azure-service-bus-transport'
import { AzureServiceBusTransportConfiguration } from './azure-service-bus-transport-configuration'
import * as uuid from 'uuid'

function getEnvVar(key: string): string {
  const value = process.env[key]
  if (!value) {
    throw new Error(`Env var not set - ${key}`)
  }
  return value
}

const AZURE_SERVICE_BUS_CONNECTION_STRING = getEnvVar(
  'AZURE_SERVICE_BUS_CONNECTION_STRING'
)

const resourcePrefix = `integration-bus-azure-${uuid.v4().substring(0, 8)}`
const queueName = `${resourcePrefix}-test`
const deadLetterQueueName = `${resourcePrefix}-dead-letter`

const configuration: AzureServiceBusTransportConfiguration = {
  connectionString: AZURE_SERVICE_BUS_CONNECTION_STRING,
  queueName,
  deadLetterQueueName,
  maxDeliveryCount: 10,
  receiveTimeoutMs: 5000,
  autoCreateResources: true
}

const systemMessageTopicName = normalizeTopicName(TestSystemMessage.NAME)

jest.setTimeout(60000)

describe('AzureServiceBusTransport', () => {
  const client = new ServiceBusClient(AZURE_SERVICE_BUS_CONNECTION_STRING)
  const adminClient = new ServiceBusAdministrationClient(
    AZURE_SERVICE_BUS_CONNECTION_STRING
  )
  const azureTransport = new AzureServiceBusTransport(
    configuration,
    client,
    adminClient
  )

  beforeAll(async () => {
    // Ensure the system message topic exists so we can publish to it directly
    const topicExists = await adminClient.topicExists(systemMessageTopicName)
    if (!topicExists) {
      await adminClient.createTopic(systemMessageTopicName)
    }
  })

  afterAll(async () => {
    // Clean up all created resources
    try {
      // Delete subscriptions first (they depend on topics and queues)
      const topicsToClean = await collectTopicsForPrefix(
        adminClient,
        resourcePrefix,
        systemMessageTopicName
      )
      for (const topicName of topicsToClean) {
        try {
          const subs = adminClient.listSubscriptions(topicName)
          for await (const sub of subs) {
            await adminClient.deleteSubscription(
              topicName,
              sub.subscriptionName
            )
          }
          await adminClient.deleteTopic(topicName)
        } catch (e) {
          // Ignore cleanup errors
        }
      }

      // Delete the service queue
      if (await adminClient.queueExists(queueName)) {
        await adminClient.deleteQueue(queueName)
      }
    } finally {
      await client.close()
    }
  })

  /**
   * Publishes a TestSystemMessage directly to the system message topic,
   * bypassing the transport under test. This simulates an external system
   * sending a message that the service needs to handle.
   */
  const publishSystemMessage = async (
    systemMessageAttribute: string
  ): Promise<void> => {
    const message = new TestSystemMessage()
    const sender = client.createSender(systemMessageTopicName)

    const busMessage: ServiceBusMessage = {
      messageId: uuid.v4(),
      body: JSON.stringify(message),
      subject: TestSystemMessage.NAME,
      applicationProperties: {
        'attributes.systemMessage': systemMessageAttribute
      }
    }

    try {
      await sender.sendMessages(busMessage)
    } finally {
      await sender.close()
    }
  }

  /**
   * Reads all messages from the dead-letter sub-queue of the service queue,
   * deletes them, and returns the deserialized messages with their attributes.
   */
  const readAllFromDeadLetterQueue = async (): Promise<
    { message: Message; attributes: MessageAttributes }[]
  > => {
    const dlqReceiver = client.createReceiver(queueName, {
      receiveMode: 'peekLock',
      subQueueType: 'deadLetter'
    })

    try {
      const rawMessages = await dlqReceiver.receiveMessages(10, {
        maxWaitTimeInMs: 10000
      })

      const results: { message: Message; attributes: MessageAttributes }[] = []

      for (const rawMessage of rawMessages) {
        try {
          const message = JSON.parse(rawMessage.body as string) as Message
          const attributes = fromApplicationProperties(
            rawMessage.applicationProperties as Record<
              string,
              string | number | boolean
            >
          )
          results.push({ message, attributes })
          await dlqReceiver.completeMessage(rawMessage)
        } catch {
          await dlqReceiver.abandonMessage(rawMessage, undefined)
        }
      }

      return results
    } finally {
      await dlqReceiver.close()
    }
  }

  transportTests(
    azureTransport,
    publishSystemMessage,
    systemMessageTopicName,
    readAllFromDeadLetterQueue
  )
})

async function collectTopicsForPrefix(
  adminClient: ServiceBusAdministrationClient,
  resourcePrefix: string,
  ...additionalTopics: string[]
): Promise<string[]> {
  const topics: string[] = [...additionalTopics]
  const topicIterator = adminClient.listTopics()
  for await (const topic of topicIterator) {
    if (topic.name.startsWith(resourcePrefix)) {
      topics.push(topic.name)
    }
  }
  return [...new Set(topics)]
}
