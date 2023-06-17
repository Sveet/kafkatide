import KafkaTide from './kafkatide';
import { Kafka } from 'kafkajs';
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
}
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
  (Kafka as any).mockReturnValue(mockKafka);
}

describe('KafkaTide', () => {
  let tide: KafkaTide;

  beforeEach(() => {
    resetMocks();
    tide = new KafkaTide({
      brokers: []
    });
  });

  describe('produce', () => {
    it('should request a producer with options', () => {
      const produceOptions = {
        allowAutoTopicCreation: false,
        transactionTimeout: 1000
      }
      const { sendSubject } = tide.produce('demo-topic', produceOptions);
      expect(mockKafka.producer).toHaveBeenCalledWith(produceOptions);
    })
  })

  describe('consume', () => {
    it('should request a consumer with options', () => {
      const consumeOptions = {
        topic: 'demo-topic',
        config: {
          groupId: 'demo-consumer'
        },
      }
      const { message$, event$ } = tide.consume(consumeOptions);
      expect(mockKafka.consumer).toHaveBeenCalledWith(consumeOptions.config);
    })
  })
});
