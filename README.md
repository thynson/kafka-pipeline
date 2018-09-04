# kafka-pipeline

## About this library

`ConsumerGroupStream` from [kafka-node] when configured with 
`autoCommit: false`, which means it is hard to implement at-least-once 
guarantee with it. This library provide a robust, easy to use kafka message 
consumer based on `ConsumerGroup` from [kafka-node] that helps you managing 
message offsets in correct way.

Even if you don't care about at-least-once guarantee at all, 
`ConsumerGroupStream` duplicates a lot of message after rebalance. Although 
your apps need to be idempotency when handle message, this library take the 
best efforts to minimize the duplication of messages, unless error occurred
that prevent the offset to be committed, you shall NEVER see any duplication.

## Installation

[kafka-node] is A peer dependency of this library, you need to install it 
together with kafka-pipeline.

```bash
npm install kafka-pipeline kafka-node
```

## Usage

```javascript 
const {ConsumerGroupPipeline} = require('kafka-pipeline');

const consumerPipeline = new ConsumerGroupPipeline(new ConsumerGroup(consumerGroupOption), {
  messageConsumer: this.messageConsumer.bind(this),
  failedMessageConsumer: this.messageConsumer.bind(this),
  maxConsumeConcurrency: 8,
  consumeTimeout: 5000
});

// The following code is for demo purpose only, not suggesting you to do this
// in production code. But to avoid message duplication you need to ensure your
// app will quit in a graceful way that await the promise returned by 
// consumerPipeline.close() is resolved, which will do the final offset commit
process.on('SIGTERM', function () {
  consumerPipeline.close().then(function () {
    process.exit(0);
  });
});
```

## License

ISC License, see `LICENSE` file.

## TODO

1. More test
2. Consider support HighLevelConsumer?

