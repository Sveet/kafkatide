import {
  CompressionTypes,
  ConsumerConfig,
  Kafka,
  KafkaConfig,
  Message,
  ProducerConfig,
  ProducerRecord,
  RecordMetadata,
} from 'kafkajs';
import { asyncScheduler, buffer, concatMap, firstValueFrom, from, mergeMap, Observable, observeOn, Subject, Subscriber } from 'rxjs';
import { EventOutput, ConsumerMessageOutput, ConsumeParams } from './types';
import { getOffsetHandlers } from './offsets';

export default class KafkaTide {
  private kafka: Kafka;
  constructor(kafkaConfig: KafkaConfig) {
    this.kafka = new Kafka(kafkaConfig);
  }

  private getConsumer(config: ConsumerConfig) {
    return this.kafka.consumer(config);
  }

  private getProducer(config?: ProducerConfig) {
    return this.kafka.producer(config);
  }

  produce = (topic: string, producerConfig?: ProducerConfig) => {
    let producer = this.getProducer(producerConfig);
    const send = async (topic: string, messages: Message[], retries = 1): Promise<RecordMetadata[]> => {
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
          producer = this.getProducer(producerConfig);
          await producer.connect();
          return send(topic, messages, retries - 1);
        } else {
          throw err;
        }
      }
    }
    const sendComplete$ = new Subject<void>();
    const sendSubject = new Subject<ProducerRecord>();
    const event$ = new Observable<EventOutput>((subscriber) => {
      for(const event of Object.values(producer.events)){
        producer.on(event, (e)=>{
          subscriber.next({
            event,
            payload: e
          })
        })
      }
    })
    from(producer.connect()).pipe(
      concatMap(() => sendSubject),
      buffer(sendComplete$),
    ).subscribe({
      next: (records) => {
        if(records.length <= 0) return;

        return send(topic, records.reduce((acc, rec) => [...acc, ...rec.messages], []))
          .then(() => sendComplete$.next())
      },
    })
    return { sendSubject, event$ }
  }

  consume = ({
    config,
    topic,
    partition,
    offset,
  }: ConsumeParams) => {
    const { startWorkingOffset, finishWorkingOffset } = getOffsetHandlers();

    const consumer = this.getConsumer({ ...config, maxInFlightRequests: 20 });
    const run = async (subscriber: Subscriber<ConsumerMessageOutput>) => {
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
        console.log(
          `handleInputData ${config.groupId}: Seeking offset: ${offsetToSeek}, partition: ${partition}`,
        );
        consumer.seek({ topic, partition, offset: offsetToSeek });
      }
    };

    const restartConsumer = async (subscriber: Subscriber<ConsumerMessageOutput>) => {
      try {
        await consumer.disconnect();
        await run(subscriber);
      } catch (err) {
        console.error(`Failed to restart consumer ${config.groupId}: ${err}`);
      }
    };
    const message$ = new Observable<ConsumerMessageOutput>((subscriber) => {
      consumer.on('consumer.crash', (e) => {
        const eventString = `${typeof e.payload.error} ${e.payload.error} ${e.payload.error.stack}`;
        if (e.payload.restart) {
          // rebalancing sometimes runs out of internal retries and requires a consumer restart
          console.error(`Consumer ${config.groupId} received a non-retriable error: ${eventString}`);
          return restartConsumer(subscriber);
        } else {
          console.warn(`KafkaJS retriable error for ${config.groupId}: ${eventString}`);
        }
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
    const event$ = new Observable<EventOutput>((subscriber) => {
      for(const event of Object.values(consumer.events)){
        consumer.on(event, (e)=>{
          subscriber.next({
            event,
            payload: e
          })
        })
      }
    })
    return { message$, event$ }
  };
}
