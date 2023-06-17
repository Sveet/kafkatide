import { count, delay, of, take, takeUntil } from 'rxjs';
import KafkaTide from './kafkatide';
import { CompressionTypes, Kafka, Message, logLevel } from 'kafkajs';
jest.mock('kafkajs');

const mockProducer = {
  connect: jest.fn().mockImplementation(async () => await new Promise(resolve => setTimeout(resolve, 100))),
  disconnect: jest.fn().mockReturnValue(Promise.resolve()),
  send: jest.fn(),
};
const mockConsumer = {
  connect: jest.fn().mockImplementation(async () => await new Promise(resolve => setTimeout(resolve, 100))),
  disconnect: jest.fn().mockReturnValue(Promise.resolve()),
  subscribe: jest.fn(),
  seek: jest.fn(),
  run: jest.fn(),
  on: jest.fn(),
};
const mockKafka = {
  producer: jest.fn().mockReturnValue(mockProducer),
  consumer: jest.fn().mockReturnValue(mockConsumer),
};
(Kafka as jest.Mock).mockReturnValue(mockKafka);

describe('KafkaTide', () => {
  const topic = 'demo-topic';
  const groupId = 'demo-consumer';
  const messages: Message[] = [
    {key: '1', headers: {foo: 'bar'}, value: 'test message 1'},
    {key: '2', headers: {aaa: 'bbb'}, value: 'test message 2'},
    {key: '3', headers: {yyy: 'zzz'}, value: 'test message 3'},
  ];
  
  let tide: KafkaTide;

  beforeEach(() => {
    jest.clearAllMocks();
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
      const { sendSubject, disconnectSubject } = tide.produce(topic, produceOptions);
      expect(mockKafka.producer).toHaveBeenCalledWith(produceOptions);
      disconnectSubject.next();
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
      jest.clearAllMocks();
      mockConsumer.run.mockImplementationOnce(async ({eachMessage})=>{
        for(const m of messages){
          await eachMessage({ message: m, partition: 1, heartbeat: jest.fn() });
          await new Promise(resolve => setTimeout(resolve, 100));
        }
      });
    });
    it('should request a consumer with options', () => {
      const consumeOptions = {
        topic,
        config: {
          groupId
        },
      };
      const { message$, event$ } = tide.consume(consumeOptions);
      expect(mockKafka.consumer).toHaveBeenCalledWith(consumeOptions.config);
    });

    it('should subscribe to the given topic', () => {
      const { message$ } = tide.consume({topic, config: { groupId }});
      message$.subscribe(() => {
        expect(mockConsumer.subscribe).toHaveBeenCalledWith({ topic, fromBeginning: false });
      });
    });

    it('should seek to the given partition and offset if provided', async () => {
      const { message$ } = tide.consume({topic, partition: 0, offset: '1', config: { groupId }});
      message$.subscribe(() => {
        expect(mockConsumer.seek).toHaveBeenCalledWith({ topic, partition: 0, offset: '1' });
      });
      await new Promise(resolve => setTimeout(resolve, 500));
    });

    it('should return messages from the topic', async () => {
      const consumeOptions = {
        topic,
        config: {
          groupId
        },
      };
      const { message$, event$ } = tide.consume(consumeOptions);
      let i = 0;
      message$.subscribe({
        next:(message) => {
          expect(message).toBe(messages[i++]);
        },
      });
    });

    it('should disconnect when the observable is unsubscribed', async () => {
      const consumeOptions = {
        topic,
        config: {
          groupId
        },
      };
      const { message$, event$ } = tide.consume(consumeOptions);
      const subscription = message$.subscribe({
        next: jest.fn(),
        complete: jest.fn(),
      });
      subscription.unsubscribe();
      expect(mockConsumer.disconnect).toHaveBeenCalled();
    });
  });
});
