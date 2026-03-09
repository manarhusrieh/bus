import { TransportConfiguration } from '@node-ts/bus-core'

export interface AzureServiceBusTransportConfiguration
  extends TransportConfiguration {
  /**
   * The Azure Service Bus connection string. Can be found in the Azure portal under
   * the Service Bus namespace > Shared access policies.
   * @example
   * Endpoint=sb://my-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=...
   */
  connectionString: string

  /**
   * The maximum number of times a message will be delivered before being moved to the
   * dead-letter sub-queue. This maps to the Azure Service Bus `maxDeliveryCount` queue property.
   * @default 10
   */
  maxDeliveryCount?: number

  /**
   * The maximum time in milliseconds to wait for a message when calling receiveMessages.
   * Setting this lower will make the transport more responsive to shutdown signals but will
   * increase the number of polling calls made to Azure Service Bus.
   * @default 5000
   */
  receiveTimeoutMs?: number

  /**
   * Controls whether the library automatically provisions necessary Azure Service Bus resources
   * (queues, topics, and subscriptions).
   *
   * By default, this is set to 'true', meaning the library will attempt to create
   * these resources on startup.
   *
   * Set this to 'false' if your application manages resource provisioning using
   * separate tools (e.g., Terraform, Bicep, or other IaC frameworks).
   *
   * @default true
   */
  autoCreateResources?: boolean

  /**
   * A resolver function that maps a message name to an Azure Service Bus topic name.
   * By default, message names are normalized by replacing invalid characters with dashes.
   * @param messageName Name of the message to map
   * @returns A valid Azure Service Bus topic name
   * @example
   *   resolveTopicName: (messageName: string) => `production-${messageName}`
   */
  resolveTopicName?: (messageName: string) => string
}
