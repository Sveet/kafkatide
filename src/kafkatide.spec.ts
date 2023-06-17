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
    disconnect: jest.fn().mockImplementationOnce(async () => await new Promise(resolve => setTimeout(resolve, 100))),
    send: jest.fn(),
  };
  mockConsumer = {
    connect: jest.fn().mockImplementationOnce(async () => await new Promise(resolve => setTimeout(resolve, 100))),
    disconnect: jest.fn().mockImplementationOnce(async () => await new Promise(resolve => setTimeout(resolve, 100))),
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
  });
});
