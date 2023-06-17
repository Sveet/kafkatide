import { from } from "rxjs";
import KafkaTide from "../dist/src/kafkatide";

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(';')
const topic = process.env.KAFKA_TOPIC ?? 'com.kafkatide.example';

// initialize KafkaTide (KafkaJS constructor)
const { consume, produce } = new KafkaTide({
  brokers,
  clientId: 'kafkatide-example',
})

// consume messages
const { message$ } = consume({ topic, config: { groupId: 'kafkatide' } })
message$.subscribe({
  next: (m) => console.log(`received: ${m.value}`),
  complete: () => console.log(`done consuming`),
})

setTimeout(()=> {
  // produce messages
  const { sendSubject } = produce(topic)
  from(['sample 1', 'sample 2', 'sample 3']).subscribe({
    next: (m) => {
      console.log(`sending: ${m}`)
      sendSubject.next({ value: m })
    },
    complete: () => console.log(`done producing`)
  })
}, 10000)


/* Example output:

sending: sample 1
sending: sample 2
sending: sample 3
done producing
received sample 1
received sample 2
received sample 3
*/