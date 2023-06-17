import {
  Admin,
  AdminConfig,
  CompressionTypes,
  ConsumerConfig,
  Kafka,
  KafkaConfig,
  Message,
  Producer,
  ProducerConfig,
} from 'kafkajs';
import { asyncScheduler, Observable, observeOn, Subscriber } from 'rxjs';
import { GetConsumerMessagesOutput, GetConsumerMessagesParams } from './types';

export default class KafkaTide {
  private kafka: Kafka;
  constructor(kafkaConfig: KafkaConfig) {
    this.kafka = new Kafka(kafkaConfig);
  }

  private getConsumer(config: ConsumerConfig) {
    return this.kafka.consumer(config);
  }
  private _admin?: Admin;
  private getAdmin(config?: AdminConfig) {
    if (!this._admin) {
      this._admin = this.kafka.admin(config);
    }
    return this._admin;
  }

  private _producer?: Producer;
  private _producerConnecting = false;
  private async getProducer(config?: ProducerConfig) {
    if (!this._producer) {
      this._producerConnecting = true;
      this._producer = this.kafka.producer(config);
      await this._producer.connect();
      this._producerConnecting = false;
    }
    while (this._producerConnecting) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    return this._producer;
  }

  async send(topic: string, messages: Message[], retries = 1) {
    const producer = await this.getProducer();
    try {
      const response = await producer.send({
        topic: topic,
        messages: messages,
        compression: CompressionTypes.GZIP,
      });
      return response;
    } catch (err: any) {
      if (retries > 0 && `${err}`.includes('The producer is disconnected')) {
        console.warn(
          `Sending KafkaJS messages failed because Producer was disconnected. Reconnecting (${retries}) and retrying to send.`,
        );
        this._producer = undefined;
        return this.send(topic, messages, retries - 1);
      } else {
        throw err;
      }
    }
  }

  streamFromTopic = ({
    config,
    topic,
    partition,
    offset,
    recoverOffsets,
  }: GetConsumerMessagesParams) => {
    const { startWorkingOffset, finishWorkingOffset } = this.getOffsetHandlers();

    const consumer = this.getConsumer({ ...config, maxInFlightRequests: 20 });
    const run = async (subscriber: Subscriber<GetConsumerMessagesOutput>) => {
      await consumer.connect();
      await consumer.subscribe({ topic, fromBeginning: false });

      consumer.run({
        autoCommit: false,
        eachMessage: async ({ message, partition, heartbeat }) => {
          try {
            const headers = message.headers;
            const body = message.value.toString();

            await heartbeat();
            startWorkingOffset(partition, Number.parseInt(message.offset));

            subscriber.next({
              type: 'message',
              headers,
              body,
              workComplete: async () => {
                const dataToCommit = finishWorkingOffset(
                  partition,
                  Number.parseInt(message.offset),
                );
                if (dataToCommit?.offset) {
                  try {
                    await consumer.commitOffsets([
                      {
                        topic,
                        partition,
                        offset: `${dataToCommit.offset + 1}`,
                      },
                    ]);
                  } catch (err) {
                    subscriber.error(err);
                  }
                }
              },
            });
          } catch (err) {
            subscriber.error(err);
          }
        },
      });

      if (partition !== undefined && offset !== undefined) {
        let offsetToSeek = offset.toString();
        let recoveredOffset = false;
        if (recoverOffsets) {
          try {
            // Try and fetch the existing offsets for this consumerGroup
            const workerPartitionOffsets = await this.getAdmin().fetchOffsets({
              groupId: config.groupId,
              topics: [topic],
            });

            const committedOffset = workerPartitionOffsets
              ?.find((t) => t.topic == topic)
              ?.partitions?.find((p) => p.partition == partition)?.offset;

            recoveredOffset = parseInt(committedOffset) > 0;
            if (recoveredOffset) {
              offsetToSeek = committedOffset;
            }
          } catch (err) {
            console.warn(`Error trying to recover offsets for ${config.groupId} ${topic} ${err}`);
          }
        }
        console.log(
          `handleInputData ${config.groupId}: Seeking offset: ${offsetToSeek}, partition: ${partition}, recovered: ${recoveredOffset}`,
        );
        consumer.seek({ topic, partition, offset: offsetToSeek });
      }
    };

    const restartConsumer = async (subscriber: Subscriber<GetConsumerMessagesOutput>) => {
      try {
        await consumer.disconnect();
        await run(subscriber);
      } catch (err) {
        console.error(`Failed to restart consumer ${config.groupId}: ${err}`);
      }
    };
    return new Observable<GetConsumerMessagesOutput>((subscriber) => {
      consumer.on('consumer.crash', (e) => {
        const eventString = `${typeof e.payload.error} ${e.payload.error} ${e.payload.error.stack}`;
        if (e.payload.restart) {
          // rebalancing sometimes runs out of internal retries and requires consumer restart
          console.error(`Consumer ${config.groupId} received a non-retriable error: ${eventString}`);
          return restartConsumer(subscriber);
        } else {
          console.warn(`KafkaJS retriable error for ${config.groupId}: ${eventString}`);
        }
      });
      consumer.on('consumer.rebalancing', (e) => {
        console.warn(`Consumer ${config.groupId} is rebalancing.`);
        subscriber.next({
          type: 'event',
          body: 'rebalancing',
        });
      });
      run(subscriber).catch((err) => {
        console.error(`KafkaJS consumer.run threw error ${err}`);
        return restartConsumer(subscriber);
      });

      return () => {
        console.debug(`Running clean-up for consumer ${config.groupId}`);
        consumer
          .disconnect()
          .then(() => console.debug(`Disconnected consumer ${config.groupId}`))
          .catch((err) => console.error(`Error disconnecting consumer ${config.groupId} ${err}`));
      };
    }).pipe(observeOn(asyncScheduler));
  };

  private getOffsetHandlers() {
    const offsetsWorking: Map<number, { offset: number; consumerGroupId?: string }[]> = new Map();
    const offsetsFinished: Map<number, { offset: number; consumerGroupId?: string }[]> = new Map();

    const startWorkingOffset = (partition: number, offset: number, consumerGroupId?: string) => {
      const offsets = offsetsWorking.get(partition) ?? [];
      offsets.push({ offset, consumerGroupId });
      offsetsWorking.set(partition, offsets);
    };
    const finishWorkingOffset = (
      partition: number,
      offset: number,
      consumerGroupId?: string,
    ): { offset: number; consumerGroupIds: string[] } => {
      // sanity check that we have the partition, offset in our working
      if (!offsetsWorking.get(partition)) {
        return undefined;
      }

      const newOffsetsFinished = offsetsFinished.get(partition) ?? [];
      newOffsetsFinished.push({ offset, consumerGroupId });
      offsetsFinished.set(partition, newOffsetsFinished);

      let newOffsetsWorking = offsetsWorking.get(partition);
      const wasLowestOffsetWorking = offset <= Math.min(...newOffsetsWorking.map((o) => o.offset));

      newOffsetsWorking = newOffsetsWorking.filter((o) => o.offset != offset);
      offsetsWorking.set(partition, newOffsetsWorking);

      if (wasLowestOffsetWorking) {
        const lowestOffsetWorking = Math.min(...newOffsetsWorking.map((o) => o.offset));
        if (isFinite(lowestOffsetWorking)) {
          offsetsFinished.set(
            partition,
            newOffsetsFinished.filter((o) => o.offset >= lowestOffsetWorking),
          );
          return {
            offset: lowestOffsetWorking - 1,
            consumerGroupIds: newOffsetsFinished
              .filter((o) => o.offset < lowestOffsetWorking)
              .map((o) => o.consumerGroupId)
              .filter((o) => !!o),
          };
        }
        const highestOffsetFinished = Math.max(...newOffsetsFinished.map((o) => o.offset));
        if (isFinite(highestOffsetFinished)) {
          offsetsFinished.set(partition, []);
          return {
            offset: highestOffsetFinished,
            consumerGroupIds: newOffsetsFinished.map((o) => o.consumerGroupId).filter((o) => !!o),
          };
        }
        return { offset, consumerGroupIds: [consumerGroupId].filter((o) => !!o) };
      }
      return undefined;
    };
    return { startWorkingOffset, finishWorkingOffset };
  }

  async deleteConsumerGroups(consumerGroupIds: string[]) {
    if (!(consumerGroupIds?.length > 0)) return;
    try {
      await this.getAdmin().deleteGroups(consumerGroupIds);
      console.log(`Deleted consumer groups: [${consumerGroupIds.join(',')}]`);
    } catch (err) {
      console.warn(`Error deleting consumer groups [${consumerGroupIds.join(',')}]: ${err}`);
    }
  }
}
