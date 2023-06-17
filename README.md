[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://choosealicense.com/licenses/mit/) [![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](code_of_conduct.md) 

# KafkaTide

KafkaTide is a lightweight wrapper around the KafkaJS library which provides a RxJS interface for producing and consuming Kafka messages.

The goal of this project is to give the user full control of the asynchronous behavior of their code.

The underlying KafkaJS configs are exposed for maximum control, while smart defaults are chosen to accommodate average use cases.

## Installation

```bash
npm install kafkatide
```
    
## Documentation

[Documentation](documentation.md)


## Usage/Examples

### Initialize Kafka Connection
The KafkaTide constructor is identical to KafkaJS constructor.
```typescript
const { consume, produce } = new KafkaTide({
  brokers: ['broker-1'],
  clientId: 'kafkatide-example',
})
```
### Consume Messages

```typescript
const topic = 'com.kafkatide.example'
const config = {
  groupId: 'kafkatide'
}

const { message$ } = consume({ topic, config })

message$.subscribe((m) => console.log(`received: ${m.value}`))
```

### Produce Messages

```typescript
const { sendSubject, disconnectSubject } = produce(topic)

sendSubject.next('Kafka has never been easier')

disconnectSubject.next()
```

## Running Tests
This repo adheres to a code coverage threshold of 90% (lines).

To run tests, run the following command.

```bash
  npm run test
```

## Contributing

Contributions are always welcome!

See [contributing.md](contributing.md) for ways to get started.

Please adhere to this project's [code of conduct](code_of_conduct.md).

## Roadmap

TBD