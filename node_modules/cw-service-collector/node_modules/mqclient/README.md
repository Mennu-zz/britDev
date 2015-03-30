# mqclient

Client for various message queues. Currently supports only RabbitMQ.

## Example

Subscriber

```
var mqClient = require('mqclient/amqp')

mqClient.sub('channel-abc', function (err, data) {
  if (err) throw new Error(err)
  else console.log('Data:', data)
})

```

Publisher

```
var mqClient = require('mqclient/amqp')

mqClient.pub('channel-abc', 'HEY HO!', function (err, msg) {
  if (err) throw new Error(err)
  else console.log('Published:', msg)
})
```
