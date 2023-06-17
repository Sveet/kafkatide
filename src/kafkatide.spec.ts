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
  commitOffsets: jest.fn(),
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

    it('should call producer.send when sendSubject is triggered', async () => {
      const { sendSubject, disconnectSubject } = tide.produce(topic);
      sendSubject.next(messages[0]);
      await new Promise(resolve => setTimeout(resolve, 500));
      expect(mockProducer.send).toHaveBeenCalledWith({topic, messages: [messages[0]], compression: CompressionTypes.GZIP});
      disconnectSubject.next();
    });

    it('should retry sending if a disconnect error occurs', async () => {
      const { sendSubject, disconnectSubject } = tide.produce(topic);
      mockProducer.send.mockImplementationOnce(()=>{throw new Error('The producer is disconnected');});
      sendSubject.next(messages[0]);
      await new Promise(resolve => setTimeout(resolve, 500));
      expect(mockProducer.send).toHaveBeenCalledTimes(2);
      disconnectSubject.next();
    });

    it('should call producer.disconnect when disconnectSubject is triggered', () => {
      const { sendSubject, disconnectSubject } = tide.produce(topic);
      disconnectSubject.next();
      expect(mockProducer.disconnect).toHaveBeenCalled();
    });

  });

  describe('consume', () => {
    beforeEach(() => {
      jest.clearAllMocks();
      mockConsumer.run.mockImplementationOnce(async ({eachMessage})=>{
        for(let i = 0; i < messages.length; i++){
          const m = messages[i];
          await eachMessage({ message: {...m, offset: i}, partition: 1, heartbeat: jest.fn() });
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

    it('should subscribe to the given topic', async () => {
      const { message$ } = tide.consume({topic, config: { groupId }});
      message$.subscribe(() => {
        expect(mockConsumer.subscribe).toHaveBeenCalledWith({ topic, fromBeginning: false });
      });
      await new Promise(resolve => setTimeout(resolve, 500));
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
          expect(message.value).toBe(messages[i++].value);
        },
      });
      await new Promise(resolve => setTimeout(resolve, 500));
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

    it('should call consumer.commitOffsets when appropriate', async () => {
      const consumeOptions = {
        topic,
        config: {
          groupId
        },
      };
      const { message$ } = tide.consume(consumeOptions);
      message$.subscribe({
        next:(message) => {
          message.workComplete.next();
        },
      });
      await new Promise(resolve => setTimeout(resolve, 500));
      expect(mockConsumer.commitOffsets).toHaveBeenCalled();
    });
    it('should call subscriber.error if consumer.commitOffsets throws an error', async () => {
      const errorMessage = 'mocked error';
      mockConsumer.commitOffsets.mockImplementationOnce(() => {throw new Error(errorMessage);});
      const consumeOptions = {
        topic,
        config: {
          groupId
        },
      };
      const { message$ } = tide.consume(consumeOptions);
      message$.subscribe({
        next:(message) => {
          message.workComplete.next();
        },
        error: (err) => {
          expect(err.message).toBe(errorMessage);
        }
      });
      await new Promise(resolve => setTimeout(resolve, 500));
    });
  });
});
