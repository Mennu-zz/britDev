var amqp = require('amqplib')

module.exports = function () {

  // keep a reference of all the exchanges here,
  var exchanges = {}

  return function (exchange, callback) {

    if ((exchange in exchanges)) {

      exchanges[exchange].consume(exchange, function(msg) {

        return callback && callback(null, msg)

      }, {
        noAck: true
      })

    }
    else {

      amqp.connect('amqp://localhost').then(function(conn) {

        return conn.createChannel().then(function(ch) {

          var ok = ch.assertExchange(exchange, 'fanout', {durable: false});
          ok = ok.then(function() {
            return ch.assertQueue('', {exclusive: true});
          })

          ok = ok.then(function(qok) {
            return ch.bindQueue(qok.queue, exchange, '').then(function() {
              return qok.queue;
            })
          })

          ok = ok.then(function(queue) {
            return ch.consume(queue, function logMessage(msg) {
              return callback && callback(null, msg.content.toString())
            }, {noAck: true})
          })

        })
      }).then(null, callback)

    }

  }

}
