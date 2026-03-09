import {
  ServiceBusClient,
  ServiceBusAdministrationClient,
  ServiceBusSender,
  ServiceBusReceiver,
  ServiceBusReceivedMessage,
  ServiceBusMessage
} from '@azure/service-bus'
import {
  Transport,
  TransportMessage,
  CoreDependencies,
  Logger,
  TransportInitializationOptions,
  DEFAULT_DEAD_LETTER_QUEUE_NAME
} from '@node-ts/bus-core'
import {
  Command,
  Event,
  Message,
  MessageAttributes,
  MessageAttributeMap
} from '@node-ts/bus-messages'
import { AzureServiceBusTransportConfiguration } from './azure-service-bus-transport-configuration'
import * as uuid from 'uuid'

const DEFAULT_MAX_DELIVERY_COUNT = 10
const DEFAULT_RECEIVE_TIMEOUT_MS = 5000

/**
 * An Azure Service Bus transport adapter for @node-ts/bus.
 *
 * Uses the following resource model:
 * - One Service Bus **Queue** per service (identified by `queueName`)
 * - One **Topic** per message type (named after the message)
 * - One **Subscription** per (topic, service) pair that forwards messages to the service queue
 *
 * This mirrors the SNS+SQS pattern used by the SQS transport.
 */
export class AzureServiceBusTransport
  implements Transport<ServiceBusReceivedMessage>
{
  private readonly client: ServiceBusClient
  private readonly adminClient: ServiceBusAdministrationClient
  private receiver: ServiceBusReceiver
  private readonly senders: Map<string, ServiceBusSender> = new Map()

  private coreDependencies: CoreDependencies
  private logger: Logger

  private readonly autoCreateResources: boolean

  constructor(
    private readonly configuration: AzureServiceBusTransportConfiguration,
    client?: ServiceBusClient,
    adminClient?: ServiceBusAdministrationClient
  ) {
    this.client = client || new ServiceBusClient(configuration.connectionString)
    this.adminClient =
      adminClient ||
      new ServiceBusAdministrationClient(configuration.connectionString)
    this.autoCreateResources = configuration.autoCreateResources ?? true
  }

  prepare(coreDependencies: CoreDependencies): void {
    this.coreDependencies = coreDependencies
    this.logger = coreDependencies.loggerFactory(
      '@node-ts/bus-azure-service-bus:azure-service-bus-transport'
    )
  }

  async initialize({
    sendOnly
  }: TransportInitializationOptions): Promise<void> {
    if (!sendOnly) {
      await this.assertServiceQueue()
      await this.subscribeToMessages()
      this.receiver = this.client.createReceiver(this.configuration.queueName, {
        receiveMode: 'peekLock'
      })
    }
  }

  async dispose(): Promise<void> {
    if (this.receiver) {
      await this.receiver.close()
    }
    for (const sender of this.senders.values()) {
      await sender.close()
    }
    await this.client.close()
  }

  async publish<TEvent extends Event>(
    event: TEvent,
    messageAttributes?: MessageAttributes
  ): Promise<void> {
    await this.publishMessage(event, messageAttributes)
  }

  async send<TCommand extends Command>(
    command: TCommand,
    messageAttributes?: MessageAttributes
  ): Promise<void> {
    await this.publishMessage(command, messageAttributes)
  }

  async readNextMessage(): Promise<
    TransportMessage<ServiceBusReceivedMessage> | undefined
  > {
    const messages = await this.receiver.receiveMessages(1, {
      maxWaitTimeInMs:
        this.configuration.receiveTimeoutMs ?? DEFAULT_RECEIVE_TIMEOUT_MS
    })

    if (!messages.length) {
      return undefined
    }

    const raw = messages[0]

    try {
      const domainMessage = this.coreDependencies.messageSerializer.deserialize(
        raw.body as string
      )
      const attributes = fromApplicationProperties(
        raw.applicationProperties as Record<string, string | number | boolean>
      )

      this.logger.debug('Received message from Azure Service Bus', {
        messageId: raw.messageId,
        subject: raw.subject
      })

      return {
        id: raw.messageId as string,
        domainMessage,
        raw,
        attributes
      }
    } catch (error) {
      this.logger.warn(
        'Could not deserialize message from Azure Service Bus, abandoning',
        { messageId: raw.messageId, error }
      )
      await this.receiver.abandonMessage(raw, undefined)
      return undefined
    }
  }

  async deleteMessage(
    message: TransportMessage<ServiceBusReceivedMessage>
  ): Promise<void> {
    this.logger.debug('Completing (deleting) message', {
      messageId: message.raw.messageId
    })
    await this.receiver.completeMessage(message.raw)
  }

  async returnMessage(
    message: TransportMessage<ServiceBusReceivedMessage>
  ): Promise<void> {
    this.logger.debug('Abandoning (returning) message for retry', {
      messageId: message.raw.messageId,
      deliveryCount: (message.raw as any).deliveryCount
    })
    await this.receiver.abandonMessage(message.raw, undefined)
  }

  async fail(
    transportMessage: TransportMessage<ServiceBusReceivedMessage>
  ): Promise<void> {
    this.logger.debug('Dead-lettering (failing) message immediately', {
      messageId: transportMessage.raw.messageId
    })
    await this.receiver.deadLetterMessage(transportMessage.raw, undefined)
  }

  private async publishMessage(
    message: Message,
    messageAttributes: MessageAttributes = {
      attributes: {},
      stickyAttributes: {}
    }
  ): Promise<void> {
    const topicName = this.resolveTopicName(message.$name)
    await this.assertTopic(topicName)
    const sender = await this.getOrCreateSender(topicName)

    const busMessage: ServiceBusMessage = {
      messageId: uuid.v4(),
      body: this.coreDependencies.messageSerializer.serialize(message),
      subject: message.$name,
      applicationProperties: toApplicationProperties(messageAttributes)
    }

    this.logger.trace('Publishing message to Azure Service Bus topic', {
      topicName,
      messageId: busMessage.messageId,
      subject: busMessage.subject
    })

    await sender.sendMessages(busMessage)
  }

  private async assertServiceQueue(): Promise<void> {
    const { queueName } = this.configuration

    this.logger.info('Asserting Azure Service Bus queue...', { queueName })

    const exists = await this.adminClient.queueExists(queueName)

    if (!exists) {
      if (this.autoCreateResources) {
        await this.adminClient.createQueue(queueName, {
          maxDeliveryCount:
            this.configuration.maxDeliveryCount ?? DEFAULT_MAX_DELIVERY_COUNT
        })
        this.logger.info('Created Azure Service Bus queue', { queueName })
      } else {
        throw new Error(
          `Azure Service Bus queue '${queueName}' does not exist and autoCreateResources is disabled`
        )
      }
    }
  }

  private async subscribeToMessages(): Promise<void> {
    const messageNames = this.coreDependencies.handlerRegistry
      .getMessageNames()
      .concat(
        this.coreDependencies.handlerRegistry.getExternallyManagedTopicIdentifiers()
      )

    this.logger.info('Subscribing Azure Service Bus queue to message topics', {
      queueName: this.configuration.queueName,
      messageNames
    })

    await Promise.all(
      messageNames.map(async messageName => {
        const topicName = this.resolveTopicName(messageName)
        await this.assertTopicSubscription(
          topicName,
          this.configuration.queueName
        )
      })
    )
  }

  private async assertTopic(topicName: string): Promise<void> {
    const exists = await this.adminClient.topicExists(topicName)

    if (!exists) {
      if (this.autoCreateResources) {
        await this.adminClient.createTopic(topicName)
        this.logger.info('Created Azure Service Bus topic', { topicName })
      } else {
        throw new Error(
          `Azure Service Bus topic '${topicName}' does not exist and autoCreateResources is disabled`
        )
      }
    }
  }

  private async assertTopicSubscription(
    topicName: string,
    subscriptionName: string
  ): Promise<void> {
    await this.assertTopic(topicName)

    const normalizedSubscriptionName =
      normalizeSubscriptionName(subscriptionName)
    const exists = await this.adminClient.subscriptionExists(
      topicName,
      normalizedSubscriptionName
    )

    if (!exists) {
      if (this.autoCreateResources) {
        await this.adminClient.createSubscription(
          topicName,
          normalizedSubscriptionName,
          { forwardTo: this.configuration.queueName }
        )
        this.logger.info('Created Azure Service Bus subscription', {
          topicName,
          subscriptionName: normalizedSubscriptionName,
          forwardTo: this.configuration.queueName
        })
      } else {
        throw new Error(
          `Azure Service Bus subscription '${normalizedSubscriptionName}' on topic '${topicName}' does not exist and autoCreateResources is disabled`
        )
      }
    }
  }

  private async getOrCreateSender(
    topicName: string
  ): Promise<ServiceBusSender> {
    if (!this.senders.has(topicName)) {
      const sender = this.client.createSender(topicName)
      this.senders.set(topicName, sender)
    }
    return this.senders.get(topicName)!
  }

  private resolveTopicName(messageName: string): string {
    if (this.configuration.resolveTopicName) {
      return this.configuration.resolveTopicName(messageName)
    }
    return normalizeTopicName(messageName)
  }
}

/**
 * Normalizes a message name into a valid Azure Service Bus topic name.
 * Azure Service Bus topic names must be 1-260 characters and contain only
 * letters, numbers, periods (.), hyphens (-), underscores (_), and tildes (~).
 */
export function normalizeTopicName(messageName: string): string {
  return messageName
    .replace(/@/g, '')
    .replace(/\//g, '-')
    .replace(/[^a-zA-Z0-9._~-]/g, '-')
    .replace(/^[-]+/, '')
    .replace(/[-]+$/, '')
    .toLowerCase()
}

/**
 * Normalizes a queue name into a valid Azure Service Bus subscription name.
 * Subscription names have a maximum length of 50 characters.
 */
export function normalizeSubscriptionName(name: string): string {
  const normalized = name
    .replace(/[^a-zA-Z0-9._~-]/g, '-')
    .replace(/^[-]+/, '')
    .replace(/[-]+$/, '')
    .toLowerCase()
  return normalized.substring(0, 50)
}

/**
 * Converts a @node-ts/bus-messages MessageAttributes object to Azure Service Bus
 * applicationProperties format.
 */
export function toApplicationProperties(
  messageAttributes: MessageAttributes
): Record<string, string | number | boolean> {
  const props: Record<string, string | number | boolean> = {}

  if (messageAttributes.correlationId) {
    props.correlationId = messageAttributes.correlationId
  }

  if (messageAttributes.attributes) {
    for (const [key, value] of Object.entries(messageAttributes.attributes)) {
      if (value !== undefined) {
        props[`attributes.${key}`] = value as string | number | boolean
      }
    }
  }

  if (messageAttributes.stickyAttributes) {
    for (const [key, value] of Object.entries(
      messageAttributes.stickyAttributes
    )) {
      if (value !== undefined) {
        props[`stickyAttributes.${key}`] = value as string | number | boolean
      }
    }
  }

  return props
}

/**
 * Converts Azure Service Bus applicationProperties back to a @node-ts/bus-messages
 * MessageAttributes object.
 */
export function fromApplicationProperties(
  props: Record<string, string | number | boolean> | undefined
): MessageAttributes {
  const messageAttributes: MessageAttributes = {
    attributes: {},
    stickyAttributes: {}
  }

  if (!props) {
    return messageAttributes
  }

  messageAttributes.correlationId = props.correlationId as string | undefined

  const attributes: MessageAttributeMap = {}
  const stickyAttributes: MessageAttributeMap = {}

  for (const [key, value] of Object.entries(props)) {
    if (key.startsWith('attributes.')) {
      const attrKey = key.substring('attributes.'.length)
      attributes[attrKey] = value as string | number | boolean
    } else if (key.startsWith('stickyAttributes.')) {
      const attrKey = key.substring('stickyAttributes.'.length)
      stickyAttributes[attrKey] = value as string | number | boolean
    }
  }

  messageAttributes.attributes = attributes
  messageAttributes.stickyAttributes = stickyAttributes

  return messageAttributes
}
