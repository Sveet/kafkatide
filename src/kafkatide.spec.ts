import { count, delay, of, take, takeUntil } from 'rxjs';
import KafkaTide from './kafkatide';
import { CompressionTypes, Kafka, Message, logLevel } from 'kafkajs';
jest.mock('kafkajs');

let mockProducer: {
  connect: jest.Mock,
  disconnect: jest.Mock,
  send: jest.Mock,
};
let mockConsumer: {
  connect: jest.Mock,
  disconnect: jest.Mock,
  subscribe: jest.Mock,
  run: jest.Mock,
  on: jest.Mock,
};
let mockKafka: {
  producer: jest.Mock,
  consumer: jest.Mock,
};
const resetMocks = () => {
  mockProducer = {
    connect: jest.fn().mockImplementationOnce(async () => await new Promise(resolve => setTimeout(resolve, 100))),
    disconnect: jest.fn(),
    send: jest.fn(),
  };
  mockConsumer = {
    connect: jest.fn().mockImplementationOnce(async () => await new Promise(resolve => setTimeout(resolve, 100))),
    disconnect: jest.fn(),
    subscribe: jest.fn(),
    run: jest.fn(),
    on: jest.fn(),
  };
  mockKafka = {
    producer: jest.fn().mockReturnValue(mockProducer),
    consumer: jest.fn().mockReturnValue(mockConsumer),
  };
  (Kafka as jest.Mock).mockReturnValue(mockKafka);
};

describe('KafkaTide', () => {
  const topic = 'demo-topic';
  const messages: Message[] = [
    {key: '1', headers: {foo: 'bar'}, value: 'test message 1'},
    {key: '2', headers: {aaa: 'bbb'}, value: 'test message 2'},
    {key: '3', headers: {yyy: 'zzz'}, value: 'test message 3'},
  ];
  
  let tide: KafkaTide;

  beforeEach(() => {
    resetMocks();
    tide = new KafkaTide({
      brokers: []
    });
  });

  it('should set log level to WARN by default', ()=>{
    expect(Kafka).toHaveBeenCalledWith({
      brokers: [],
      logLevel: logLevel.WARN
    });
  });

  describe('produce', () => {
    it('should request a producer with options', () => {
      const produceOptions = {
        allowAutoTopicCreation: false,
        transactionTimeout: 1000
      };
      const { sendSubject } = tide.produce(topic, produceOptions);
      expect(mockKafka.producer).toHaveBeenCalledWith(produceOptions);
    });

    it('calls producer.send sendSubject.next(messages) is called', async () => {
      const { sendSubject, disconnectSubject } = tide.produce(topic);
      sendSubject.next(messages);
      await new Promise(resolve => setTimeout(resolve, 500));
      expect(mockProducer.send).toHaveBeenCalledWith({topic, messages, compression: CompressionTypes.GZIP});
      disconnectSubject.next();
    });

  });

  describe('consume', () => {
    beforeEach(() => {
      resetMocks();
      mockConsumer.run.mockImplementationOnce(async ({eachMessage})=>{
        for(const m of messages){
          await eachMessage({ message: m, partition: 1, heartbeat: jest.fn() })
          await new Promise(resolve => setTimeout(resolve, 100))
        }
      })
    })
    it('should request a consumer with options', () => {
      const consumeOptions = {
        topic: 'demo-topic',
        config: {
          groupId: 'demo-consumer'
        },
      };
      const { message$, event$ } = tide.consume(consumeOptions);
      expect(mockKafka.consumer).toHaveBeenCalledWith(consumeOptions.config);
    });

    it('should return a message from the topic', async () => {
      const consumeOptions = {
        topic: 'demo-topic',
        config: {
          groupId: 'demo-consumer'
        },
      };
      const { message$, event$ } = tide.consume(consumeOptions);
      let i = 0
      message$.pipe(
        takeUntil(of([true]).pipe(delay(1000))),
      ).subscribe({
        next:(message) => {
          expect(message).toBe(messages[i++])
        },
      })
      await new Promise(resolve => setTimeout(resolve, 1000));
    })
  });
});
