import 'reflect-metadata'
import {
  AzureServiceBusTransport,
  toApplicationProperties,
  fromApplicationProperties,
  normalizeTopicName
} from './azure-service-bus-transport'
import { MessageAttributes } from '@node-ts/bus-messages'
import * as faker from 'faker'
import { AzureServiceBusTransportConfiguration } from './azure-service-bus-transport-configuration'
import {
  CoreDependencies,
  TransportMessage,
  DebugLogger
} from '@node-ts/bus-core'
import { Mock, It, Times } from 'typemoq'
import {
  ServiceBusClient,
  ServiceBusAdministrationClient,
  ServiceBusSender,
  ServiceBusReceiver,
  ServiceBusReceivedMessage
} from '@azure/service-bus'

const makeConfig = (
  overrides: Partial<AzureServiceBusTransportConfiguration> = {}
): AzureServiceBusTransportConfiguration => ({
  connectionString:
    'Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=dGVzdA==',
  queueName: 'test-queue',
  ...overrides
})

describe('azure-service-bus-transport', () => {
  describe('normalizeTopicName', () => {
    it('should replace @ and / with dashes', () => {
      expect(normalizeTopicName('@node-ts/bus-messages:TestEvent')).toEqual(
        'node-ts-bus-messages-testevent'
      )
    })

    it('should replace spaces with dashes', () => {
      expect(normalizeTopicName('some message name')).toEqual(
        'some-message-name'
      )
    })

    it('should strip leading dashes', () => {
      expect(normalizeTopicName('@prefix/name')).toMatch(/^[^-]/)
    })

    it('should lowercase the result', () => {
      const result = normalizeTopicName('MyMessage')
      expect(result).toEqual(result.toLowerCase())
    })

    it('should allow dots, hyphens and underscores', () => {
      expect(normalizeTopicName('my.message_name-v1')).toEqual(
        'my.message_name-v1'
      )
    })
  })

  describe('when converting message attributes to application properties', () => {
    const correlationId = faker.random.uuid()
    const messageOptions: MessageAttributes = {
      correlationId,
      attributes: {
        attribute1: 'a',
        attribute2: 1,
        attribute3: true
      },
      stickyAttributes: {
        sticky1: 'b',
        sticky2: 2
      }
    }

    let applicationProperties: Record<string, string | number | boolean>

    beforeEach(() => {
      applicationProperties = toApplicationProperties(messageOptions)
    })

    it('should include the correlationId', () => {
      expect(applicationProperties.correlationId).toEqual(correlationId)
    })

    it('should include string attributes with prefix', () => {
      expect(applicationProperties['attributes.attribute1']).toEqual('a')
    })

    it('should include numeric attributes with prefix', () => {
      expect(applicationProperties['attributes.attribute2']).toEqual(1)
    })

    it('should include boolean attributes with prefix', () => {
      expect(applicationProperties['attributes.attribute3']).toEqual(true)
    })

    it('should include sticky attributes with prefix', () => {
      expect(applicationProperties['stickyAttributes.sticky1']).toEqual('b')
      expect(applicationProperties['stickyAttributes.sticky2']).toEqual(2)
    })

    it('should not include undefined values', () => {
      const options: MessageAttributes = {
        attributes: { key: undefined as unknown as string },
        stickyAttributes: {}
      }
      const result = toApplicationProperties(options)
      expect(result['attributes.key']).toBeUndefined()
    })
  })

  describe('when converting application properties to message attributes', () => {
    const correlationId = faker.random.uuid()

    const applicationProperties: Record<string, string | number | boolean> = {
      correlationId,
      'attributes.attribute1': 'a',
      'attributes.attribute2': 1,
      'attributes.attribute3': true,
      'stickyAttributes.sticky1': 'b',
      'stickyAttributes.sticky2': 2
    }

    let messageAttributes: MessageAttributes

    beforeEach(() => {
      messageAttributes = fromApplicationProperties(applicationProperties)
    })

    it('should parse the correlation id', () => {
      expect(messageAttributes.correlationId).toEqual(correlationId)
    })

    it('should parse string attributes', () => {
      expect(messageAttributes.attributes['attribute1']).toEqual('a')
    })

    it('should parse numeric attributes', () => {
      expect(messageAttributes.attributes['attribute2']).toEqual(1)
    })

    it('should parse boolean attributes', () => {
      expect(messageAttributes.attributes['attribute3']).toEqual(true)
    })

    it('should parse sticky attributes', () => {
      expect(messageAttributes.stickyAttributes['sticky1']).toEqual('b')
      expect(messageAttributes.stickyAttributes['sticky2']).toEqual(2)
    })

    it('should return empty attributes when applicationProperties is undefined', () => {
      const result = fromApplicationProperties(undefined)
      expect(result.attributes).toEqual({})
      expect(result.stickyAttributes).toEqual({})
      expect(result.correlationId).toBeUndefined()
    })
  })

  describe('when deleting a message', () => {
    it('should complete the message via the receiver', async () => {
      const mockReceiver = Mock.ofType<ServiceBusReceiver>()
      const mockClient = Mock.ofType<ServiceBusClient>()
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()

      const rawMessage = {} as ServiceBusReceivedMessage
      const transportMessage: TransportMessage<ServiceBusReceivedMessage> = {
        id: 'test-id',
        domainMessage: {} as any,
        raw: rawMessage,
        attributes: { attributes: {}, stickyAttributes: {} }
      }

      mockReceiver
        .setup(r => r.completeMessage(It.isAny()))
        .returns(() => Promise.resolve())
        .verifiable(Times.once())

      const sut = new AzureServiceBusTransport(
        makeConfig(),
        mockClient.object,
        mockAdminClient.object
      )
      sut.prepare({
        loggerFactory: (name: string) => new DebugLogger(name)
      } as any as CoreDependencies)
      sut['receiver'] = mockReceiver.object

      await sut.deleteMessage(transportMessage)
      mockReceiver.verifyAll()
    })
  })

  describe('when returning a message to the queue', () => {
    it('should abandon the message via the receiver', async () => {
      const mockReceiver = Mock.ofType<ServiceBusReceiver>()
      const mockClient = Mock.ofType<ServiceBusClient>()
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()

      const rawMessage = {} as ServiceBusReceivedMessage
      const transportMessage: TransportMessage<ServiceBusReceivedMessage> = {
        id: 'test-id',
        domainMessage: {} as any,
        raw: rawMessage,
        attributes: { attributes: {}, stickyAttributes: {} }
      }

      mockReceiver
        .setup(r => r.abandonMessage(It.isAny(), It.isAny()))
        .returns(() => Promise.resolve())
        .verifiable(Times.once())

      const sut = new AzureServiceBusTransport(
        makeConfig(),
        mockClient.object,
        mockAdminClient.object
      )
      sut.prepare({
        loggerFactory: (name: string) => new DebugLogger(name)
      } as any as CoreDependencies)
      sut['receiver'] = mockReceiver.object

      await sut.returnMessage(transportMessage)
      mockReceiver.verifyAll()
    })
  })

  describe('when failing a message', () => {
    it('should dead-letter the message via the receiver', async () => {
      const mockReceiver = Mock.ofType<ServiceBusReceiver>()
      const mockClient = Mock.ofType<ServiceBusClient>()
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()

      const rawMessage = {} as ServiceBusReceivedMessage
      const transportMessage: TransportMessage<ServiceBusReceivedMessage> = {
        id: 'test-id',
        domainMessage: {} as any,
        raw: rawMessage,
        attributes: { attributes: {}, stickyAttributes: {} }
      }

      mockReceiver
        .setup(r => r.deadLetterMessage(It.isAny(), It.isAny()))
        .returns(() => Promise.resolve())
        .verifiable(Times.once())

      const sut = new AzureServiceBusTransport(
        makeConfig(),
        mockClient.object,
        mockAdminClient.object
      )
      sut.prepare({
        loggerFactory: (name: string) => new DebugLogger(name)
      } as any as CoreDependencies)
      sut['receiver'] = mockReceiver.object

      await sut.fail(transportMessage)
      mockReceiver.verifyAll()
    })
  })

  describe('autoCreateResources configuration', () => {
    it('should default to true when not specified', () => {
      const mockClient = Mock.ofType<ServiceBusClient>()
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()
      const config = makeConfig()

      const sut = new AzureServiceBusTransport(
        config,
        mockClient.object,
        mockAdminClient.object
      )
      expect(sut['autoCreateResources']).toBe(true)
    })

    it('should use the configured value when set to false', () => {
      const mockClient = Mock.ofType<ServiceBusClient>()
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()
      const config = makeConfig({ autoCreateResources: false })

      const sut = new AzureServiceBusTransport(
        config,
        mockClient.object,
        mockAdminClient.object
      )
      expect(sut['autoCreateResources']).toBe(false)
    })
  })

  describe('when autoCreateResources is true', () => {
    it('should create the queue when initializing', async () => {
      const mockClient = Mock.ofType<ServiceBusClient>()
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()
      const mockReceiver = Mock.ofType<ServiceBusReceiver>()
      const config = makeConfig({ autoCreateResources: true })

      mockAdminClient
        .setup(a => a.queueExists('test-queue'))
        .returns(() => Promise.resolve(false))

      mockAdminClient
        .setup(a =>
          a.createQueue(
            It.is(name => name === 'test-queue'),
            It.isAny()
          )
        )
        .returns(() => Promise.resolve({} as any))
        .verifiable(Times.once())

      mockAdminClient
        .setup(a => a.topicExists(It.isAnyString()))
        .returns(() => Promise.resolve(true))

      mockAdminClient
        .setup(a => a.subscriptionExists(It.isAnyString(), It.isAnyString()))
        .returns(() => Promise.resolve(true))

      mockClient
        .setup(c => c.createReceiver(It.isAnyString(), It.isAny()))
        .returns(() => mockReceiver.object as any)

      const sut = new AzureServiceBusTransport(
        config,
        mockClient.object,
        mockAdminClient.object
      )
      sut.prepare({
        loggerFactory: (name: string) => new DebugLogger(name),
        handlerRegistry: {
          getMessageNames: () => [],
          getExternallyManagedTopicIdentifiers: () => []
        }
      } as any as CoreDependencies)

      await sut.initialize({ sendOnly: false, handlerRegistry: {} as any })
      mockAdminClient.verifyAll()
    })

    it('should create a topic when it does not exist', async () => {
      const mockClient = Mock.ofType<ServiceBusClient>()
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()
      const mockSender = Mock.ofType<ServiceBusSender>()
      const config = makeConfig({ autoCreateResources: true })

      mockAdminClient
        .setup(a => a.topicExists(It.isAnyString()))
        .returns(() => Promise.resolve(false))
        .verifiable(Times.once())

      mockAdminClient
        .setup(a => a.createTopic(It.isAnyString()))
        .returns(() => Promise.resolve({} as any))
        .verifiable(Times.once())

      mockClient
        .setup(c => c.createSender(It.isAnyString()))
        .returns(() => mockSender.object as any)

      mockSender
        .setup(s => s.sendMessages(It.isAny()))
        .returns(() => Promise.resolve())

      const sut = new AzureServiceBusTransport(
        config,
        mockClient.object,
        mockAdminClient.object
      )
      sut.prepare({
        loggerFactory: (name: string) => new DebugLogger(name),
        messageSerializer: {
          serialize: () => '{}',
          deserialize: () => ({} as any)
        }
      } as any as CoreDependencies)

      await sut['assertTopic']('my-topic')
      mockAdminClient.verifyAll()
    })

    it('should create a subscription when it does not exist', async () => {
      const mockClient = Mock.ofType<ServiceBusClient>()
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()
      const config = makeConfig({ autoCreateResources: true })

      mockAdminClient
        .setup(a => a.topicExists(It.isAnyString()))
        .returns(() => Promise.resolve(true))

      mockAdminClient
        .setup(a => a.subscriptionExists(It.isAnyString(), It.isAnyString()))
        .returns(() => Promise.resolve(false))
        .verifiable(Times.once())

      mockAdminClient
        .setup(a =>
          a.createSubscription(It.isAnyString(), It.isAnyString(), It.isAny())
        )
        .returns(() => Promise.resolve({} as any))
        .verifiable(Times.once())

      const sut = new AzureServiceBusTransport(
        config,
        mockClient.object,
        mockAdminClient.object
      )
      sut.prepare({
        loggerFactory: (name: string) => new DebugLogger(name)
      } as any as CoreDependencies)

      await sut['assertTopicSubscription']('my-topic', 'test-queue')
      mockAdminClient.verifyAll()
    })
  })

  describe('when autoCreateResources is false', () => {
    it('should throw when queue does not exist', async () => {
      const mockClient = Mock.ofType<ServiceBusClient>()
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()
      const config = makeConfig({ autoCreateResources: false })

      mockAdminClient
        .setup(a => a.queueExists('test-queue'))
        .returns(() => Promise.resolve(false))

      const sut = new AzureServiceBusTransport(
        config,
        mockClient.object,
        mockAdminClient.object
      )
      sut.prepare({
        loggerFactory: (name: string) => new DebugLogger(name),
        handlerRegistry: {
          getMessageNames: () => [],
          getExternallyManagedTopicIdentifiers: () => []
        }
      } as any as CoreDependencies)

      await expect(
        sut.initialize({ sendOnly: false, handlerRegistry: {} as any })
      ).rejects.toThrow('does not exist and autoCreateResources is disabled')
    })

    it('should throw when topic does not exist', async () => {
      const mockClient = Mock.ofType<ServiceBusClient>()
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()
      const config = makeConfig({ autoCreateResources: false })

      mockAdminClient
        .setup(a => a.topicExists(It.isAnyString()))
        .returns(() => Promise.resolve(false))

      const sut = new AzureServiceBusTransport(
        config,
        mockClient.object,
        mockAdminClient.object
      )
      sut.prepare({
        loggerFactory: (name: string) => new DebugLogger(name)
      } as any as CoreDependencies)

      await expect(sut['assertTopic']('my-topic')).rejects.toThrow(
        'does not exist'
      )
    })

    it('should throw when subscription does not exist', async () => {
      const mockClient = Mock.ofType<ServiceBusClient>()
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()
      const config = makeConfig({ autoCreateResources: false })

      mockAdminClient
        .setup(a => a.topicExists(It.isAnyString()))
        .returns(() => Promise.resolve(true))

      mockAdminClient
        .setup(a => a.subscriptionExists(It.isAnyString(), It.isAnyString()))
        .returns(() => Promise.resolve(false))

      const sut = new AzureServiceBusTransport(
        config,
        mockClient.object,
        mockAdminClient.object
      )
      sut.prepare({
        loggerFactory: (name: string) => new DebugLogger(name)
      } as any as CoreDependencies)

      await expect(
        sut['assertTopicSubscription']('my-topic', 'test-queue')
      ).rejects.toThrow('does not exist')
    })
  })

  describe('when reading the next message', () => {
    it('should return undefined when no messages are available', async () => {
      const mockReceiver = Mock.ofType<ServiceBusReceiver>()
      const mockClient = Mock.ofType<ServiceBusClient>()
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()

      mockReceiver
        .setup(r => r.receiveMessages(1, It.isAny()))
        .returns(() => Promise.resolve([]))

      const sut = new AzureServiceBusTransport(
        makeConfig(),
        mockClient.object,
        mockAdminClient.object
      )
      sut.prepare({
        loggerFactory: (name: string) => new DebugLogger(name)
      } as any as CoreDependencies)
      sut['receiver'] = mockReceiver.object

      const result = await sut.readNextMessage()
      expect(result).toBeUndefined()
    })

    it('should deserialize and return the message when one is available', async () => {
      const mockReceiver = Mock.ofType<ServiceBusReceiver>()
      const mockClient = Mock.ofType<ServiceBusClient>()
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()

      const correlationId = faker.random.uuid()
      const body = JSON.stringify({ $name: 'TestMessage', $version: 1 })
      const rawMessage: Partial<ServiceBusReceivedMessage> = {
        messageId: 'msg-123',
        body,
        applicationProperties: {
          correlationId,
          'attributes.key1': 'val1'
        }
      }

      mockReceiver
        .setup(r => r.receiveMessages(1, It.isAny()))
        .returns(() =>
          Promise.resolve([rawMessage as ServiceBusReceivedMessage])
        )

      const domainMessage = { $name: 'TestMessage', $version: 1 }
      const mockSerializer = {
        serialize: () => body,
        deserialize: () => domainMessage as any
      }

      const sut = new AzureServiceBusTransport(
        makeConfig(),
        mockClient.object,
        mockAdminClient.object
      )
      sut.prepare({
        loggerFactory: (name: string) => new DebugLogger(name),
        messageSerializer: mockSerializer
      } as any as CoreDependencies)
      sut['receiver'] = mockReceiver.object

      const result = await sut.readNextMessage()
      expect(result).toBeDefined()
      expect(result!.id).toEqual('msg-123')
      expect(result!.domainMessage).toEqual(domainMessage)
      expect(result!.attributes.correlationId).toEqual(correlationId)
      expect(result!.attributes.attributes['key1']).toEqual('val1')
    })

    it('should abandon and return undefined when deserialization fails', async () => {
      const mockReceiver = Mock.ofType<ServiceBusReceiver>()
      const mockClient = Mock.ofType<ServiceBusClient>()
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()

      const rawMessage = {
        messageId: 'bad-msg',
        body: 'invalid-json'
      } as any as ServiceBusReceivedMessage

      mockReceiver
        .setup(r => r.receiveMessages(1, It.isAny()))
        .returns(() => Promise.resolve([rawMessage]))

      mockReceiver
        .setup(r => r.abandonMessage(It.isAny(), It.isAny()))
        .returns(() => Promise.resolve())
        .verifiable(Times.once())

      const mockSerializer = {
        serialize: () => '',
        deserialize: () => {
          throw new Error('Deserialization failed')
        }
      }

      const sut = new AzureServiceBusTransport(
        makeConfig(),
        mockClient.object,
        mockAdminClient.object
      )
      sut.prepare({
        loggerFactory: (name: string) => new DebugLogger(name),
        messageSerializer: mockSerializer
      } as any as CoreDependencies)
      sut['receiver'] = mockReceiver.object

      const result = await sut.readNextMessage()
      expect(result).toBeUndefined()
      mockReceiver.verifyAll()
    })
  })

  describe('when publishing a message', () => {
    // Use plain jest mocks for ServiceBusClient since TypeMoq does not proxy
    // ServiceBusClient.createSender() reliably (it's a concrete class that
    // lazily connects, which can cause tests to hang on sendMessages()).

    it('should send to the correct topic', async () => {
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()
      const config = makeConfig()

      const testMessage = {
        $name: '@node-ts/test:TestEvent',
        $version: 1
      }

      mockAdminClient
        .setup(a => a.topicExists(It.isAnyString()))
        .returns(() => Promise.resolve(true))

      const sendMessages = jest.fn().mockResolvedValue(undefined)
      const createSender = jest
        .fn()
        .mockReturnValue({ sendMessages, close: jest.fn() })
      const mockClient = {
        createSender,
        close: jest.fn()
      } as any as ServiceBusClient

      const sut = new AzureServiceBusTransport(
        config,
        mockClient,
        mockAdminClient.object
      )
      sut.prepare({
        loggerFactory: (name: string) => new DebugLogger(name),
        messageSerializer: {
          serialize: () => JSON.stringify(testMessage),
          deserialize: () => testMessage as any
        }
      } as any as CoreDependencies)

      await sut.publish(testMessage as any)
      expect(createSender).toHaveBeenCalledWith('node-ts-test-testevent')
      expect(sendMessages).toHaveBeenCalledTimes(1)
    })

    it('should use a custom resolveTopicName when provided', async () => {
      const mockAdminClient = Mock.ofType<ServiceBusAdministrationClient>()
      const config = makeConfig({
        resolveTopicName: (name: string) => `prod-${name}`
      })

      const testEvent = { $name: 'TestEvent', $version: 1 }

      mockAdminClient
        .setup(a => a.topicExists(It.isAnyString()))
        .returns(() => Promise.resolve(true))

      const sendMessages = jest.fn().mockResolvedValue(undefined)
      const createSender = jest
        .fn()
        .mockReturnValue({ sendMessages, close: jest.fn() })
      const mockClient = {
        createSender,
        close: jest.fn()
      } as any as ServiceBusClient

      const sut = new AzureServiceBusTransport(
        config,
        mockClient,
        mockAdminClient.object
      )
      sut.prepare({
        loggerFactory: (name: string) => new DebugLogger(name),
        messageSerializer: {
          serialize: () => JSON.stringify(testEvent),
          deserialize: () => testEvent as any
        }
      } as any as CoreDependencies)

      await sut.publish(testEvent as any)
      expect(createSender).toHaveBeenCalledWith('prod-TestEvent')
      expect(sendMessages).toHaveBeenCalledTimes(1)
    })
  })
})
